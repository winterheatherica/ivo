package engine

import (
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	netizenv1 "ivo/gen/ivo/netizen/v1"
)

var testPersonas = Generate(5000)

func samplePost() Post {
	return Post{
		PostID:       uuid.MustParse("4f1c2d7e-9a55-5b0e-8c3d-2e6f8a9b1c0d"),
		PublishedAt:  time.Date(2026, 11, 9, 12, 30, 0, 0, time.UTC), // 19:30 WIB
		RegionCode:   "ID-JK",
		StyleTags:    []string{"casual", "street", "cheerful"},
		Followers:    1200,
		NoveltyBP:    10000,
		PenaltyBP:    10000,
		RulesVersion: 1,
	}
}

func TestReactIsDeterministic(t *testing.T) {
	a := React(samplePost(), testPersonas, Params{DelayScaleBP: 10000})
	// The order of the input personas must not affect the result.
	reversed := make([]Persona, len(testPersonas))
	for i, p := range testPersonas {
		reversed[len(testPersonas)-1-i] = p
	}
	b := React(samplePost(), reversed, Params{DelayScaleBP: 10000})
	if !reflect.DeepEqual(a, b) {
		t.Fatal("React must give identical results for the same input")
	}
}

func TestReactTotalsMatchBuckets(t *testing.T) {
	res := React(samplePost(), testPersonas, Params{DelayScaleBP: 10000})
	var likes, comments int64
	for _, b := range res.Buckets {
		likes += int64(b.Likes)
		comments += int64(b.Comments)
		if b.EffectiveAt.Before(b.StartsAt) || !b.EffectiveAt.Before(b.StartsAt.Add(15*time.Minute)) {
			t.Fatalf("effective_at %v is outside bucket %v", b.EffectiveAt, b.StartsAt)
		}
	}
	// A bucket's numbers must not apply before the last comment in it happens.
	for _, c := range res.Comments {
		for _, b := range res.Buckets {
			if !c.PlannedAt.Before(b.StartsAt) && c.PlannedAt.Before(b.StartsAt.Add(15*time.Minute)) && c.PlannedAt.After(b.EffectiveAt) {
				t.Fatalf("comment at %v happens after the bucket's effective_at %v", c.PlannedAt, b.EffectiveAt)
			}
		}
	}
	if likes != res.TotalLikes || comments != res.TotalComments || int64(len(res.Comments)) != res.TotalComments {
		t.Fatalf("totals don't match: likes %d/%d, comments %d/%d/%d",
			likes, res.TotalLikes, comments, res.TotalComments, len(res.Comments))
	}
	// A rough range so accidental weight changes get noticed.
	if res.TotalLikes < 50 || res.TotalLikes > 2000 || res.TotalComments < 5 || res.TotalComments > 300 {
		t.Fatalf("reaction counts outside a reasonable range: likes %d, comments %d", res.TotalLikes, res.TotalComments)
	}
}

func TestReactRespectsDelayCapAndScale(t *testing.T) {
	post := samplePost()
	full := React(post, testPersonas, Params{DelayScaleBP: 10000})
	for _, c := range full.Comments {
		if c.PlannedAt.Before(post.PublishedAt) || c.PlannedAt.After(post.PublishedAt.Add(4*time.Hour)) {
			t.Fatalf("comment outside the 4 hours: %v", c.PlannedAt)
		}
	}
	fast := React(post, testPersonas, Params{DelayScaleBP: 100})
	if fast.TotalComments != full.TotalComments || fast.TotalLikes != full.TotalLikes {
		t.Fatal("the time scale must not change the reaction counts")
	}
	for _, c := range fast.Comments {
		if c.PlannedAt.After(post.PublishedAt.Add(3 * time.Minute)) {
			t.Fatalf("at a 1/100 scale, comments must appear within 2.4 minutes: %v", c.PlannedAt)
		}
	}
}

// Every publish hour must still get comments, even though quiet hours get far
// fewer than busy ones.
func TestEveryHourGetsComments(t *testing.T) {
	post := samplePost()
	post.Followers = 0
	quiet, busy := int64(1<<62), int64(0)
	for h := 0; h < 24; h++ {
		post.PublishedAt = time.Date(2026, 11, 9, h, 0, 0, 0, time.UTC)
		res := React(post, testPersonas, Params{})
		if res.TotalComments == 0 {
			t.Fatalf("%02d:00 UTC got no comments at all", h)
		}
		t.Logf("%02d:00 WIB: %d likes, %d comments", (h+7)%24, res.TotalLikes, res.TotalComments)
		quiet = min(quiet, res.TotalLikes+res.TotalComments)
		busy = max(busy, res.TotalLikes+res.TotalComments)
	}
	if busy < 2*quiet {
		t.Fatalf("the busy hour (%d reactions) should be well above the quiet hour (%d)", busy, quiet)
	}
}

func TestNoveltyAndPenaltyReduceReactions(t *testing.T) {
	base := React(samplePost(), testPersonas, Params{})
	tired := samplePost()
	tired.NoveltyBP, tired.PenaltyBP = 2000, 5000
	low := React(tired, testPersonas, Params{})
	if low.TotalLikes+low.TotalComments >= base.TotalLikes+base.TotalComments {
		t.Fatal("low novelty and the frequency penalty must reduce reactions")
	}
}

func TestChunksGroupByVoiceAndSize(t *testing.T) {
	res := React(samplePost(), testPersonas, Params{})
	chunks := Chunks(res.Comments, 15)
	seen := 0
	for i, c := range chunks {
		if c.No != int32(i) || len(c.Comments) == 0 || len(c.Comments) > 15 {
			t.Fatalf("chunk %d is invalid: no=%d, size=%d", i, c.No, len(c.Comments))
		}
		for _, p := range c.Comments {
			if p.Voice != c.Voice {
				t.Fatalf("chunk %d mixes voices", i)
			}
		}
		seen += len(c.Comments)
	}
	if seen != len(res.Comments) {
		t.Fatalf("the chunks cover %d comments, want %d", seen, len(res.Comments))
	}
}

func TestGenerateIsDeterministicAndValid(t *testing.T) {
	again := Generate(5000)
	if !reflect.DeepEqual(testPersonas, again) {
		t.Fatal("Generate must be deterministic")
	}
	handle := regexp.MustCompile(`^[a-z0-9_.]{3,30}$`)
	ids := map[uuid.UUID]bool{}
	trueChars := 0
	for _, p := range testPersonas {
		if !handle.MatchString(p.Handle) {
			t.Fatalf("invalid handle: %q", p.Handle)
		}
		if ids[p.ID] {
			t.Fatalf("duplicate ID: %s", p.ID)
		}
		ids[p.ID] = true
		for _, h := range p.ActiveHours {
			if h.From < 0 || h.From > 23 || h.To < 0 || h.To > 23 {
				t.Fatalf("invalid active hours: %+v", h)
			}
		}
		if p.VoiceDesc == "" {
			t.Fatalf("persona type %q has no voice description", p.Type)
		}
		if p.TrueCharacter != "" {
			trueChars++
		}
	}
	if trueChars < 500 || trueChars > 1000 {
		t.Fatalf("about 15%% of personas should have a true character, got %d", trueChars)
	}
}

func TestRenderFillsEveryPlaceholder(t *testing.T) {
	ctx := PostContext{IdolName: "Rara", DestinationName: "Kota Tua Jakarta", OutfitName: "Casual denim jacket", PoseName: "Standing and waving"}
	for i, p := range testPersonas[:500] {
		voice := netizenv1.CommentVoice_COMMENT_VOICE_REGULAR
		if i%3 == 0 {
			voice = netizenv1.CommentVoice_COMMENT_VOICE_TRUE_CHARACTER
		}
		text := Render(p, ctx, voice, uuid.New())
		if text == "" || strings.ContainsAny(text, "{}") || len([]rune(text)) > 200 {
			t.Fatalf("invalid text: %q", text)
		}
	}
}

// Every persona type must have templates for both voices, so a type rename
// can't silently fall back to another type's templates.
func TestEveryPersonaTypeHasTemplates(t *testing.T) {
	for _, typ := range personaTypes {
		if len(regularTemplates[typ]) == 0 || len(trueCharacterTemplates[typ]) == 0 {
			t.Fatalf("persona type %q is missing templates", typ)
		}
		if voiceByType[typ] == "" || trueCharacterByType[typ] == "" {
			t.Fatalf("persona type %q is missing a voice description", typ)
		}
	}
}
