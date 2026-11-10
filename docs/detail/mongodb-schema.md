# MongoDB schema

Only the netizen service uses MongoDB ([ADR-0020](../adr/0020-mongodb-for-netizens.md)). Database: `netizen`.

- MongoDB runs as a single node without a replica set, because netizen doesn't use multi-document transactions ([ADR-0013](../adr/0013-deterministic-netizen-idempotency.md)).
- The WiredTiger cache is capped with `--wiredTigerCacheSizeGB 0.25`, so RAM usage stays at 0.4–0.5 GB.
- IDs use BSON UUIDs (binary subtype 4), with the same values as the IDs in other services.
- Every collection has a `$jsonSchema` validator with `validationAction: "error"`, so the database rejects malformed documents.

The scripts below are written for `mongosh`; netizen's migration applies the same validators and indexes.

## `personas`

The 5,000 netizen personas. The list of `type` values (for example simp, critic, and place-focused) is decided together with the story, so the validator only checks the shape.

```js
db.createCollection("personas", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["_id", "handle", "display_name", "type", "interests",
                 "active_hours", "voice", "persona_version"],
      properties: {
        _id: { bsonType: "binData", description: "netizen_id (UUID)" },
        handle: { bsonType: "string", pattern: "^[a-z0-9_.]{3,30}$" },
        display_name: { bsonType: "string", maxLength: 50 },
        type: { bsonType: "string", maxLength: 30 },
        interests: {
          bsonType: "object",
          required: ["regions", "styles"],
          properties: {
            regions: { bsonType: "array", items: { bsonType: "string" } },
            styles: { bsonType: "array", items: { bsonType: "string" } }
          }
        },
        active_hours: {
          bsonType: "array",
          minItems: 1,
          items: {
            bsonType: "object",
            required: ["from_hour", "to_hour"],
            properties: {
              from_hour: { bsonType: "int", minimum: 0, maximum: 23 },
              to_hour: { bsonType: "int", minimum: 0, maximum: 23 }
            }
          }
        },
        voice: {
          bsonType: "object",
          required: ["description"],
          properties: {
            description: { bsonType: "string", maxLength: 500 },
            emoji_rate_bp: { bsonType: "int", minimum: 0, maximum: 10000 },
            slang: { bsonType: "array", items: { bsonType: "string" } }
          }
        },
        true_character: {
          bsonType: ["object", "null"],
          required: ["description"],
          properties: { description: { bsonType: "string", maxLength: 500 } }
        },
        persona_version: { bsonType: "int", minimum: 1 }
      }
    }
  },
  validationAction: "error"
});
db.personas.createIndex({ type: 1 });
```

## `comments`

Comments whose text is ready. A comment is only stored once its text exists, from the LLM or a template, so the app can never read a half-finished document.

```js
db.createCollection("comments", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["_id", "post_id", "player_id", "netizen_id", "text", "source", "route",
                 "prompt_version", "planned_at", "visible_at", "created_at"],
      properties: {
        _id: { bsonType: "binData", description: "comment_id = UUIDv5(NS_COMMENT, post_id:netizen_id)" },
        post_id: { bsonType: "binData" },
        player_id: { bsonType: "binData" },
        netizen_id: { bsonType: "binData" },
        text: { bsonType: "string", minLength: 1, maxLength: 200 },
        source: { enum: ["llm", "template"] },
        route: { enum: ["cloudflare", "modal", "xai", "template"] },
        model: { bsonType: ["string", "null"] },
        prompt_version: { bsonType: "string" },
        moderation: {
          bsonType: "object",
          properties: {
            score_bp: { bsonType: "int", minimum: 0, maximum: 10000 },
            blocked_reason: { bsonType: ["string", "null"] }
          }
        },
        planned_at: { bsonType: "date" },
        visible_at: { bsonType: "date", description: "the later of planned_at and the time the text was ready" },
        created_at: { bsonType: "date" }
      }
    }
  },
  validationAction: "error"
});
db.comments.createIndex({ post_id: 1, netizen_id: 1 }, { unique: true });
db.comments.createIndex({ post_id: 1, visible_at: 1, _id: 1 });
db.comments.createIndex({ player_id: 1 });
```

The comment list query for the app:

```js
db.comments.find(
  { post_id: postId, visible_at: { $lte: now } },
  { text: 1, netizen_id: 1, visible_at: 1 }
).sort({ visible_at: 1, _id: 1 }).limit(50);
```

## `post_runs`

A marker that a post's reactions have been decided, together with their summary.

```js
db.createCollection("post_runs", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["_id", "player_id", "status", "rules_version", "persona_version",
                 "chunk_count", "chunks_done", "totals", "created_at", "updated_at"],
      properties: {
        _id: { bsonType: "binData", description: "post_id" },
        player_id: { bsonType: "binData" },
        status: { enum: ["reacting", "reacted", "commented"] },
        rules_version: { bsonType: "int", minimum: 1 },
        persona_version: { bsonType: "int", minimum: 1 },
        chunk_count: { bsonType: "int", minimum: 0 },
        chunks_done: { bsonType: "array", items: { bsonType: "int" } },
        totals: {
          bsonType: "object",
          required: ["likes", "comments"],
          properties: {
            likes: { bsonType: ["int", "long"], minimum: 0 },
            comments: { bsonType: ["int", "long"], minimum: 0 }
          }
        },
        first_comment_at: { bsonType: ["date", "null"] },
        created_at: { bsonType: "date" },
        updated_at: { bsonType: "date" }
      }
    }
  },
  validationAction: "error"
});
db.post_runs.createIndex({ player_id: 1 });
```

The document is created with the status `reacting` before any event is sent, so the comment workers always find it. If netizen dies before the status changes to `reacted`, reprocessing sends identical events once more. The status becomes `commented` when `chunks_done` holds as many entries as `chunk_count`. Workers add chunk numbers with `$addToSet`, so a chunk processed twice is never counted twice.

## `comment_backlog`

Chunks waiting for the hourly Modal batch. They always hold regular comments, because true-character chunks go straight to xAI.

```js
db.createCollection("comment_backlog", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["_id", "post_id", "player_id", "chunk_no", "comments", "status",
                 "enqueued_at", "attempts"],
      properties: {
        _id: { bsonType: "string", description: "<post_id>:<chunk_no>" },
        post_id: { bsonType: "binData" },
        player_id: { bsonType: "binData" },
        chunk_no: { bsonType: "int", minimum: 0 },
        comments: {
          bsonType: "array",
          minItems: 1,
          maxItems: 20,
          items: {
            bsonType: "object",
            required: ["netizen_id", "planned_at"],
            properties: {
              netizen_id: { bsonType: "binData" },
              planned_at: { bsonType: "date" }
            }
          }
        },
        status: { enum: ["pending", "running", "done"] },
        enqueued_at: { bsonType: "date" },
        run_id: { bsonType: ["string", "null"] },
        attempts: { bsonType: "int", minimum: 0 },
        done_at: { bsonType: ["date", "null"] }
      }
    }
  },
  validationAction: "error"
});
db.comment_backlog.createIndex({ status: 1, enqueued_at: 1 });
db.comment_backlog.createIndex({ player_id: 1 });
db.comment_backlog.createIndex(
  { done_at: 1 },
  { expireAfterSeconds: 604800, partialFilterExpression: { status: "done" } }
);
```

Documents that are `done` are deleted automatically by the TTL index after 7 days.

## Account deletion

When it receives `player.deletion.requested`, netizen deletes the player's documents in `comments`, `post_runs`, and `comment_backlog` through the `player_id` index, then publishes `player.deletion.completed`. Netizen personas are not deleted because they belong to the system.
