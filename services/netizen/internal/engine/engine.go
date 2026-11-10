// Package engine decides how netizens react to a post, deterministically: the
// same input always produces the same reactions, byte for byte
// (docs/detail/flow-3-publish-and-reactions.md#2-netizen-decides-the-reactions).
// The package touches neither the database nor the network, so it is easy to test.
package engine

import (
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"

	netizenv1 "ivo/gen/ivo/netizen/v1"
	"ivo/pkg/clock"
	"ivo/pkg/detrand"
)

// maxDelayMinutes: a netizen who couldn't react until more than 4 hours after
// publishing is treated as never having seen the post.
const maxDelayMinutes = 240

// HourRange is a range of active hours in WIB, inclusive. From > To means it wraps past midnight.
type HourRange struct{ From, To int }

// Persona is a netizen loaded into memory.
type Persona struct {
	ID            uuid.UUID
	Handle        string
	DisplayName   string
	Type          string
	Regions       []string
	Styles        []string
	ActiveHours   []HourRange
	VoiceDesc     string
	EmojiRateBP   int32
	Slang         []string
	TrueCharacter string // empty if the persona has no true character
}

// ActiveAt reports whether the persona is active at a given WIB hour.
func (p Persona) ActiveAt(hour int) bool {
	for _, r := range p.ActiveHours {
		if r.From <= r.To {
			if hour >= r.From && hour <= r.To {
				return true
			}
		} else if hour >= r.From || hour <= r.To {
			return true
		}
	}
	return false
}

// minutesUntilActive returns the minutes from t until the start of the next active hour.
func (p Persona) minutesUntilActive(t time.Time) int {
	local := t.In(clock.WIB)
	for h := 1; h <= 24; h++ {
		if p.ActiveAt((local.Hour() + h) % 24) {
			return h*60 - local.Minute()
		}
	}
	return math.MaxInt32
}

// Post is the input frozen in the post.published payload.
type Post struct {
	PostID       uuid.UUID
	PublishedAt  time.Time
	RegionCode   string
	StyleTags    []string
	Followers    int64
	NoveltyBP    int32
	PenaltyBP    int32
	RulesVersion int32
}

// Params are settings outside the payload. DelayScaleBP speeds up time for dev
// environments: 10,000 means real time, 100 means 100 times faster.
type Params struct {
	DelayScaleBP int32
}

// Planned is one planned comment.
type Planned struct {
	NetizenID uuid.UUID
	PlannedAt time.Time
	Voice     netizenv1.CommentVoice
}

// Bucket counts the likes and comments in one 15-minute bucket.
// EffectiveAt is the time of the last reaction in the bucket: the bucket's
// numbers only apply from then on, so they never run ahead of the reactions.
type Bucket struct {
	StartsAt    time.Time
	EffectiveAt time.Time
	Likes       int32
	Comments    int32
}

// Result holds the reaction decisions for one post.
type Result struct {
	Buckets        []Bucket  // sorted by StartsAt
	Comments       []Planned // sorted by voice, planned_at, then netizen_id
	TotalLikes     int64
	TotalComments  int64
	FirstCommentAt *time.Time
}

// React decides how every persona reacts to post. Every float expression that
// affects the outcome is wrapped in a float64(...) conversion so the compiler
// can't fuse it into an FMA (docs/detail/README.md#deterministic-randomness).
func React(post Post, personas []Persona, params Params) Result {
	sorted := append([]Persona(nil), personas...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID.String() < sorted[j].ID.String() })

	hour := post.PublishedAt.In(clock.WIB).Hour()
	followerTerm := float64(0.35 * math.Log10(float64(post.Followers)+10))
	multiplier := float64(float64(post.NoveltyBP)/10000) * float64(float64(post.PenaltyBP)/10000)
	scale := float64(params.DelayScaleBP) / 10000
	if params.DelayScaleBP <= 0 {
		scale = 1
	}

	buckets := map[time.Time]*Bucket{}
	var res Result
	for _, n := range sorted {
		material := post.PostID.String() + ":" + n.ID.String() + ":" + strconv.Itoa(int(post.RulesVersion))
		active := n.ActiveAt(hour)

		z := float64(-3.4)
		if contains(n.Regions, post.RegionCode) {
			z = float64(z + 1.2)
		}
		z = float64(z + float64(0.6*float64(min(overlap(n.Styles, post.StyleTags), 2))))
		if active {
			z = float64(z + 0.8)
		}
		z = float64(z + followerTerm)
		pReact := float64(sigmoid(z) * multiplier)
		pComment := float64(pReact * 0.12)

		u := detrand.U(material, 0)
		if u >= pReact {
			continue
		}
		delay := float64(-math.Log(1-detrand.U(material, 1)) * 40) // 40 minutes on average
		if !active {
			delay = float64(delay + float64(n.minutesUntilActive(post.PublishedAt)))
		}
		if delay > maxDelayMinutes {
			continue
		}
		at := post.PublishedAt.Add(time.Duration(float64(delay * scale * float64(time.Minute))))
		start := clock.BucketStart(at)
		b, ok := buckets[start]
		if !ok {
			b = &Bucket{StartsAt: start}
			buckets[start] = b
		}
		if at.After(b.EffectiveAt) {
			b.EffectiveAt = at
		}
		if u < pComment {
			voice := netizenv1.CommentVoice_COMMENT_VOICE_REGULAR
			if n.TrueCharacter != "" && detrand.U(material, 2) < 0.25 {
				voice = netizenv1.CommentVoice_COMMENT_VOICE_TRUE_CHARACTER
			}
			res.Comments = append(res.Comments, Planned{NetizenID: n.ID, PlannedAt: at, Voice: voice})
			b.Comments++
			res.TotalComments++
		} else {
			b.Likes++
			res.TotalLikes++
		}
	}

	for _, b := range buckets {
		res.Buckets = append(res.Buckets, *b)
	}
	sort.Slice(res.Buckets, func(i, j int) bool { return res.Buckets[i].StartsAt.Before(res.Buckets[j].StartsAt) })
	sort.Slice(res.Comments, func(i, j int) bool {
		a, b := res.Comments[i], res.Comments[j]
		if a.Voice != b.Voice {
			return a.Voice < b.Voice
		}
		if !a.PlannedAt.Equal(b.PlannedAt) {
			return a.PlannedAt.Before(b.PlannedAt)
		}
		return a.NetizenID.String() < b.NetizenID.String()
	})
	for _, c := range res.Comments {
		if res.FirstCommentAt == nil || c.PlannedAt.Before(*res.FirstCommentAt) {
			t := c.PlannedAt
			res.FirstCommentAt = &t
		}
	}
	return res
}

// Chunk is a group of comments with the same voice.
type Chunk struct {
	No       int32
	Voice    netizenv1.CommentVoice
	Comments []Planned
}

// Chunks splits the (already sorted) comments by voice, at most size per chunk.
func Chunks(comments []Planned, size int) []Chunk {
	var out []Chunk
	for _, c := range comments {
		last := len(out) - 1
		if last < 0 || out[last].Voice != c.Voice || len(out[last].Comments) >= size {
			out = append(out, Chunk{No: int32(len(out)), Voice: c.Voice})
			last = len(out) - 1
		}
		out[last].Comments = append(out[last].Comments, c)
	}
	return out
}

func sigmoid(z float64) float64 { return 1 / (1 + math.Exp(-z)) }

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func overlap(a, b []string) int {
	n := 0
	for _, x := range a {
		if contains(b, x) {
			n++
		}
	}
	return n
}
