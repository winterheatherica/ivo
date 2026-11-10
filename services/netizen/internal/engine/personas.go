package engine

import (
	"fmt"
	"strings"

	"ivo/pkg/detrand"
	"ivo/pkg/ids"
)

// These persona types and interests are provisional for M1; the final list is
// decided together with the story.
var (
	personaTypes = []string{"simp", "critic", "place_focused", "fashion", "local"}
	regionCodes  = []string{"ID-JK", "ID-JB", "ID-YO", "ID-BA"}
	styleTags    = []string{"casual", "street", "feminine", "summer", "traditional", "batik",
		"sporty", "cheerful", "relaxed", "cafe", "candid", "playful", "elegant"}
	firstNames = []string{"Ayu", "Bima", "Citra", "Dimas", "Eka", "Fajar", "Gita", "Hana", "Indra",
		"Joko", "Kirana", "Lestari", "Maya", "Nanda", "Oki", "Putri", "Raka", "Sari", "Tari",
		"Umar", "Vina", "Wulan", "Yoga", "Zahra"}
	handleSuffixes = []string{"", "_id", ".jkt", "_xx", "chan", "_real", "kun", ".art"}
	slangWords     = []string{"omg", "ngl", "fr", "bestie", "lowkey", "slay", "no cap"}

	voiceByType = map[string]string{
		"simp":          "Over-the-top praise, lots of heart emojis.",
		"critic":        "Critical and blunt, rarely gives praise.",
		"place_focused": "More interested in the location than in the idol.",
		"fashion":       "Pays attention to outfits and style details.",
		"local":         "Lives near the location, friendly, and likes giving recommendations.",
	}
	trueCharacterByType = map[string]string{
		"simp":          "A fanatical fan who writes in all caps.",
		"critic":        "A sharp critic who is never satisfied.",
		"place_focused": "An amateur tour guide who always corrects facts about the location.",
		"fashion":       "A stylist who judges every detail of the outfit.",
		"local":         "A neighborhood elder who treats every idol like a grandchild.",
	}
)

// PersonaVersion goes up every time Generate changes. Version 2 added early
// birds and night owls; version 3 moved the persona types and voices to English.
const PersonaVersion = 3

// Generate creates n personas deterministically from their index.
func Generate(n int) []Persona {
	out := make([]Persona, n)
	for i := 0; i < n; i++ {
		m := fmt.Sprintf("persona:%d", i)
		typ := personaTypes[detrand.Intn(m, 0, len(personaTypes))]
		first := firstNames[detrand.Intn(m, 1, len(firstNames))]
		suffix := handleSuffixes[detrand.Intn(m, 2, len(handleSuffixes))]
		handle := fmt.Sprintf("%s%s%02d", strings.ToLower(first), suffix, detrand.Intn(m, 3, 100))

		regions := []string{regionCodes[detrand.Intn(m, 4, len(regionCodes))]}
		if detrand.U(m, 5) < 0.3 {
			regions = appendUnique(regions, regionCodes[detrand.Intn(m, 6, len(regionCodes))])
		}
		styles := []string{styleTags[detrand.Intn(m, 7, len(styleTags))]}
		styles = appendUnique(styles, styleTags[detrand.Intn(m, 8, len(styleTags))])

		// Active hours in WIB. Early birds and night owls make sure every hour
		// still has active netizens, even though far fewer than at peak hours.
		var start, length int
		switch r := detrand.U(m, 9); {
		case r < 0.15: // early birds: start between 04:00 and 06:00
			start, length = 4+detrand.Intn(m, 16, 3), 3+detrand.Intn(m, 10, 3)
		case r < 0.30: // night owls: start between 21:00 and 23:00, past midnight
			start, length = 21+detrand.Intn(m, 16, 3), 4+detrand.Intn(m, 10, 3)
		default: // start between 06:00 and 19:00
			start, length = 6+detrand.Intn(m, 16, 14), 2+detrand.Intn(m, 10, 5)
		}
		hours := []HourRange{{From: start, To: (start + length) % 24}}
		if detrand.U(m, 11) < 0.4 {
			hours = append(hours, HourRange{From: 11, To: 13}) // lunch break
		}

		p := Persona{
			ID:          ids.NetizenID(i),
			Handle:      handle,
			DisplayName: fmt.Sprintf("%s %c.", first, 'A'+rune(detrand.Intn(m, 12, 26))),
			Type:        typ,
			Regions:     regions,
			Styles:      styles,
			ActiveHours: hours,
			VoiceDesc:   voiceByType[typ],
			EmojiRateBP: int32(detrand.Intn(m, 13, 6000)),
			Slang:       []string{slangWords[detrand.Intn(m, 14, len(slangWords))]},
		}
		if detrand.U(m, 15) < 0.15 {
			p.TrueCharacter = trueCharacterByType[typ]
		}
		out[i] = p
	}
	return out
}

func appendUnique(list []string, v string) []string {
	if contains(list, v) {
		return list
	}
	return append(list, v)
}
