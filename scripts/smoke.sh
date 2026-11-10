#!/usr/bin/env bash
# M1 end-to-end test: one post is created, goes live, and then gets netizen comments.
# Needs curl and Python 3. Run it once Docker Compose is up:
#   bash scripts/smoke.sh
set -euo pipefail

API="${API:-http://localhost:8080}"
# Pick a Python that actually runs (on Windows, "python3" can be the Microsoft Store alias).
PY=""
for candidate in python3 python; do
  if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c 'import sys' >/dev/null 2>&1; then
    PY="$candidate"
    break
  fi
done
[[ -n "$PY" ]] || { echo "FAILED: Python 3 not found" >&2; exit 1; }
export PYTHONIOENCODING=utf-8 # netizen comments contain emoji; Windows defaults to cp1252
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

uuid() { "$PY" -c "import uuid; print(uuid.uuid4())"; }
field() { "$PY" -c "import json,sys; print(json.load(open(sys.argv[1], encoding='utf-8'))$2)" "$1"; }
fail() { echo "FAILED: $*" >&2; exit 1; }

# request METHOD PATH [BODY] [IDEMPOTENCY_KEY] -> writes the body to $TMP/out, prints the HTTP status
request() {
  local args=(-s -o "$TMP/out" -w '%{http_code}' -X "$1" "$API$2" -H "X-Player-Id: $PLAYER")
  [[ -n "${3:-}" ]] && args+=(-H 'Content-Type: application/json' -d "$3")
  [[ -n "${4:-}" ]] && args+=(-H "Idempotency-Key: $4")
  curl "${args[@]}"
}

echo "Waiting for the gateway at $API ..."
for _ in $(seq 1 60); do
  curl -fs "$API/healthz" >/dev/null 2>&1 && break
  sleep 2
done
curl -fs "$API/healthz" >/dev/null || fail "the gateway isn't responding"

PLAYER="$(uuid)"
KEY="$(uuid)"
BODY='{"destination_id":"id-jakarta-kota-tua","outfit_id":"casual-denim","pose_id":"standing-wave","expression_id":"smile","caption":"Afternoon at Kota Tua!"}'

code="$(request POST /v1/posts "$BODY" "$KEY")"
[[ "$code" == 201 ]] || fail "POST /v1/posts = $code: $(cat "$TMP/out")"
POST_ID="$(field "$TMP/out" "['post_id']")"
echo "1. Post created: $POST_ID (status $(field "$TMP/out" "['status']"))"

code="$(request POST /v1/posts "$BODY" "$KEY")"
[[ "$code" == 200 && "$(field "$TMP/out" "['post_id']")" == "$POST_ID" ]] || fail "idempotent replay = $code"
echo "2. Resent with the same Idempotency-Key: 200, same post"

code="$(request POST /v1/posts '{"destination_id":"id-jakarta-kota-tua","outfit_id":"summer-dress","pose_id":"standing-wave","expression_id":"smile","caption":"Different body"}' "$KEY")"
[[ "$code" == 422 ]] || fail "same Idempotency-Key with a different body = $code, want 422"
echo "3. Same Idempotency-Key with a different body: 422 $(field "$TMP/out" "['code']")"

echo "4. Waiting for the photo to be ready and the post to go live (1-minute render estimate in dev) ..."
for _ in $(seq 1 90); do
  request GET "/v1/posts/$POST_ID" >/dev/null
  [[ "$(field "$TMP/out" "['published']")" == True ]] && break
  sleep 2
done
[[ "$(field "$TMP/out" "['published']")" == True ]] || fail "the post isn't live yet: $(cat "$TMP/out")"
echo "   Live at $(field "$TMP/out" "['published_at']")"

echo "5. Waiting for netizen comments ..."
for _ in $(seq 1 90); do
  request GET "/v1/posts/$POST_ID/comments" >/dev/null
  [[ "$(field "$TMP/out" "['comments'].__len__()")" -gt 0 ]] && break
  sleep 2
done
n="$(field "$TMP/out" "['comments'].__len__()")"
[[ "$n" -gt 0 ]] || fail "no comments yet"
echo "   $n comments visible. Examples:"
"$PY" -c "
import json, sys
for c in json.load(open(sys.argv[1], encoding='utf-8'))['comments'][:5]:
    print('   - @%s: %s' % (c['netizen_handle'], c['text']))
" "$TMP/out"

echo "6. Waiting for the engagement numbers (a bucket applies once its last reaction happens) ..."
for _ in $(seq 1 120); do
  request GET "/v1/posts/$POST_ID" >/dev/null
  [[ "$(field "$TMP/out" "['likes']")" -gt 0 ]] && break
  sleep 2
done
likes="$(field "$TMP/out" "['likes']")"
[[ "$likes" -gt 0 ]] || fail "the like count hasn't gone up"
echo "   $likes likes, $(field "$TMP/out" "['comments']") comments"

echo "7. Waiting for followers to go up after the next fold ..."
for _ in $(seq 1 45); do
  request GET /v1/me/popularity >/dev/null
  [[ "$(field "$TMP/out" "['followers']")" -gt 0 ]] && break
  sleep 2
done
followers="$(field "$TMP/out" "['followers']")"
[[ "$followers" -gt 0 ]] || fail "followers haven't gone up"
echo "   Followers: $followers, season score: $(field "$TMP/out" "['season_score']")"
echo "PASSED"
