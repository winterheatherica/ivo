package engine

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"

	netizenv1 "ivo/gen/ivo/netizen/v1"
	"ivo/pkg/detrand"
)

// TemplateVersion is recorded on every comment as its prompt_version.
const TemplateVersion = "template-v2"

// PostContext holds the text from game.GetPostContext that the templates use.
type PostContext struct {
	IdolName        string
	DestinationName string
	OutfitName      string
	PoseName        string
}

// Templates are the fallback path in code: used when the LLM fails or the
// budget runs out (design §10), and as the only path in M1.
var (
	regularTemplates = map[string][]string{
		"simp": {
			"{idol} looks so pretty at {destination}!",
			"Every post from {idol} makes my day.",
			"That {outfit} suits you perfectly, {idol}.",
			"I'm already waiting for the next post!",
		},
		"critic": {
			"Hmm, the {outfit} doesn't really fit the vibe of {destination}.",
			"Hmm, {pose} again? Mix it up a little.",
			"The photo's okay, but it could be better.",
			"Pretty average. The last post was better.",
		},
		"place_focused": {
			"{destination} is at its best around this time of day.",
			"Whoa, {destination}! Any food recommendations around there?",
			"I've been to {destination}, and it feels exactly like that.",
			"The {destination} backdrop really brings the photo to life.",
		},
		"fashion": {
			"The {outfit} plus {pose}, a perfect combo.",
			"The details on that {outfit} are so clean, who's the stylist?",
			"The outfit colors work so well with {destination}.",
			"This is {idol}'s best look so far.",
		},
		"local": {
			"Oh, {destination}! That's close to my place.",
			"Welcome to {destination}, {idol}!",
			"Come visit {destination} again sometime.",
			"People here must be happy to see you at {destination}.",
		},
	}
	trueCharacterTemplates = map[string][]string{
		"simp": {
			"I LOVE YOU {IDOL}!!!",
			"This post belongs in a museum, period.",
		},
		"critic": {
			"Honestly, this is the most boring post of the week.",
			"{destination} again? Out of ideas already?",
		},
		"place_focused": {
			"Small correction: the best spot in {destination} is two blocks from there.",
			"Fact: {destination} looks best during golden hour.",
		},
		"fashion": {
			"This {outfit} needs accessories. I'd give it a 6 out of 10.",
			"The silhouette works, but those shoes were the wrong call.",
		},
		"local": {
			"Look at {idol}, all grown up and traveling all the way to {destination}.",
			"Don't forget to eat, {idol} dear. {destination} gets hot.",
		},
	}
	emojis = []string{"😍", "✨", "🔥", "👏", "💕", "😆", "🙌"}
)

// Render builds the comment text deterministically from the comment_id.
func Render(p Persona, c PostContext, voice netizenv1.CommentVoice, commentID uuid.UUID) string {
	pool := regularTemplates[p.Type]
	if voice == netizenv1.CommentVoice_COMMENT_VOICE_TRUE_CHARACTER {
		pool = trueCharacterTemplates[p.Type]
	}
	if len(pool) == 0 {
		pool = regularTemplates["place_focused"]
	}
	m := commentID.String()
	text := strings.NewReplacer(
		"{idol}", c.IdolName,
		"{IDOL}", strings.ToUpper(c.IdolName),
		"{destination}", c.DestinationName,
		"{outfit}", lowerFirst(c.OutfitName),
		"{pose}", lowerFirst(c.PoseName),
	).Replace(pool[detrand.Intn(m, 0, len(pool))])

	if len(p.Slang) > 0 && detrand.U(m, 1) < 0.3 {
		text = p.Slang[detrand.Intn(m, 2, len(p.Slang))] + ", " + lowerFirst(text)
	}
	if detrand.U(m, 3)*10000 < float64(p.EmojiRateBP) {
		text += " " + emojis[detrand.Intn(m, 4, len(emojis))]
	}
	return truncate(text, 200)
}

func lowerFirst(s string) string {
	r, size := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError {
		return s
	}
	return string(unicode.ToLower(r)) + s[size:]
}

func truncate(s string, maxRunes int) string {
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	return string([]rune(s)[:maxRunes])
}
