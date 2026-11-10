package netizen

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"ivo/services/netizen/internal/engine"
)

// The collection validators, the same as in docs/detail/mongodb-schema.md.
//
//go:embed schemas/*.json
var schemaFiles embed.FS

const uuidSubtype = 0x04 // the BSON binary subtype for UUIDs

func bin(id uuid.UUID) bson.Binary { return bson.Binary{Subtype: uuidSubtype, Data: id[:]} }

func unbin(b bson.Binary) uuid.UUID {
	id, err := uuid.FromBytes(b.Data)
	if err != nil {
		return uuid.Nil
	}
	return id
}

// doc builds a bson.D from key and value pairs.
func doc(kv ...any) bson.D {
	out := make(bson.D, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, bson.E{Key: kv[i].(string), Value: kv[i+1]})
	}
	return out
}

type personaDoc struct {
	ID             bson.Binary  `bson:"_id"`
	Handle         string       `bson:"handle"`
	DisplayName    string       `bson:"display_name"`
	Type           string       `bson:"type"`
	Interests      interestsDoc `bson:"interests"`
	ActiveHours    []hoursDoc   `bson:"active_hours"`
	Voice          voiceDoc     `bson:"voice"`
	TrueCharacter  *trueCharDoc `bson:"true_character"`
	PersonaVersion int32        `bson:"persona_version"`
}

type interestsDoc struct {
	Regions []string `bson:"regions"`
	Styles  []string `bson:"styles"`
}

type hoursDoc struct {
	FromHour int32 `bson:"from_hour"`
	ToHour   int32 `bson:"to_hour"`
}

type voiceDoc struct {
	Description string   `bson:"description"`
	EmojiRateBP int32    `bson:"emoji_rate_bp"`
	Slang       []string `bson:"slang"`
}

type trueCharDoc struct {
	Description string `bson:"description"`
}

type moderationDoc struct {
	ScoreBP       int32   `bson:"score_bp"`
	BlockedReason *string `bson:"blocked_reason"`
}

type commentDoc struct {
	ID            bson.Binary   `bson:"_id"`
	PostID        bson.Binary   `bson:"post_id"`
	PlayerID      bson.Binary   `bson:"player_id"`
	NetizenID     bson.Binary   `bson:"netizen_id"`
	Text          string        `bson:"text"`
	Source        string        `bson:"source"`
	Route         string        `bson:"route"`
	Model         *string       `bson:"model"`
	PromptVersion string        `bson:"prompt_version"`
	Moderation    moderationDoc `bson:"moderation"`
	PlannedAt     time.Time     `bson:"planned_at"`
	VisibleAt     time.Time     `bson:"visible_at"`
	CreatedAt     time.Time     `bson:"created_at"`
}

type totalsDoc struct {
	Likes    int64 `bson:"likes"`
	Comments int64 `bson:"comments"`
}

type postRunDoc struct {
	ID             bson.Binary `bson:"_id"`
	PlayerID       bson.Binary `bson:"player_id"`
	Status         string      `bson:"status"`
	RulesVersion   int32       `bson:"rules_version"`
	PersonaVersion int32       `bson:"persona_version"`
	ChunkCount     int32       `bson:"chunk_count"`
	ChunksDone     []int32     `bson:"chunks_done"`
	Totals         totalsDoc   `bson:"totals"`
	FirstCommentAt *time.Time  `bson:"first_comment_at"`
	CreatedAt      time.Time   `bson:"created_at"`
	UpdatedAt      time.Time   `bson:"updated_at"`
}

// Store holds the MongoDB collections owned by netizen.
type Store struct {
	db       *mongo.Database
	personas *mongo.Collection
	comments *mongo.Collection
	runs     *mongo.Collection
	backlog  *mongo.Collection
}

// OpenStore connects to MongoDB, waits until it is ready (at most 60 seconds),
// then makes sure the validators and indexes are in place.
func OpenStore(ctx context.Context, uri, dbName string, log *slog.Logger) (*mongo.Client, *Store, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, nil, err
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = client.Ping(pingCtx, nil)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return nil, nil, fmt.Errorf("mongodb is unreachable: %w", err)
		}
		log.Info("waiting for mongodb", "err", err)
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	db := client.Database(dbName)
	st := &Store{
		db:       db,
		personas: db.Collection("personas"),
		comments: db.Collection("comments"),
		runs:     db.Collection("post_runs"),
		backlog:  db.Collection("comment_backlog"),
	}
	if err := st.migrate(ctx); err != nil {
		return nil, nil, err
	}
	return client, st, nil
}

func (st *Store) migrate(ctx context.Context) error {
	for _, name := range []string{"personas", "comments", "post_runs", "comment_backlog"} {
		raw, err := schemaFiles.ReadFile("schemas/" + name + ".json")
		if err != nil {
			return err
		}
		var schema bson.D
		if err := bson.UnmarshalExtJSON(raw, false, &schema); err != nil {
			return fmt.Errorf("schema %s: %w", name, err)
		}
		validator := doc("$jsonSchema", schema)
		err = st.db.CreateCollection(ctx, name,
			options.CreateCollection().SetValidator(validator).SetValidationAction("error"))
		var cmdErr mongo.CommandError
		switch {
		case err == nil:
		case errors.As(err, &cmdErr) && cmdErr.Code == 48: // NamespaceExists: update the validator
			if err := st.db.RunCommand(ctx, doc("collMod", name, "validator", validator,
				"validationAction", "error")).Err(); err != nil {
				return fmt.Errorf("collMod %s: %w", name, err)
			}
		default:
			return fmt.Errorf("creating collection %s: %w", name, err)
		}
	}

	indexes := map[*mongo.Collection][]mongo.IndexModel{
		st.personas: {{Keys: doc("type", 1)}},
		st.comments: {
			{Keys: doc("post_id", 1, "netizen_id", 1), Options: options.Index().SetUnique(true)},
			{Keys: doc("post_id", 1, "visible_at", 1, "_id", 1)},
			{Keys: doc("player_id", 1)},
		},
		st.runs: {{Keys: doc("player_id", 1)}},
		st.backlog: {
			{Keys: doc("status", 1, "enqueued_at", 1)},
			{Keys: doc("player_id", 1)},
			{Keys: doc("done_at", 1), Options: options.Index().SetExpireAfterSeconds(604800).
				SetPartialFilterExpression(doc("status", "done"))},
		},
	}
	for coll, models := range indexes {
		if _, err := coll.Indexes().CreateMany(ctx, models); err != nil {
			return fmt.Errorf("index %s: %w", coll.Name(), err)
		}
	}
	return nil
}

// SeedPersonas writes the personas from Generate when the latest persona version
// isn't complete yet. It upserts per _id, so it is safe to repeat after an
// interrupted seed, and it automatically updates personas from older versions.
func (st *Store) SeedPersonas(ctx context.Context, personas []engine.Persona) error {
	n, err := st.personas.CountDocuments(ctx, doc("persona_version", engine.PersonaVersion))
	if err != nil {
		return err
	}
	if n >= int64(len(personas)) {
		return nil
	}
	for start := 0; start < len(personas); start += 1000 {
		end := min(start+1000, len(personas))
		models := make([]mongo.WriteModel, 0, end-start)
		for _, p := range personas[start:end] {
			d := toPersonaDoc(p)
			models = append(models, mongo.NewReplaceOneModel().SetFilter(doc("_id", d.ID)).
				SetReplacement(d).SetUpsert(true))
		}
		if _, err := st.personas.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
			return fmt.Errorf("seeding personas: %w", err)
		}
	}
	return nil
}

// LoadPersonas reads every persona into memory.
func (st *Store) LoadPersonas(ctx context.Context) ([]engine.Persona, error) {
	cur, err := st.personas.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	var docs []personaDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	out := make([]engine.Persona, 0, len(docs))
	for _, d := range docs {
		p := engine.Persona{
			ID:          unbin(d.ID),
			Handle:      d.Handle,
			DisplayName: d.DisplayName,
			Type:        d.Type,
			Regions:     d.Interests.Regions,
			Styles:      d.Interests.Styles,
			VoiceDesc:   d.Voice.Description,
			EmojiRateBP: d.Voice.EmojiRateBP,
			Slang:       d.Voice.Slang,
		}
		for _, h := range d.ActiveHours {
			p.ActiveHours = append(p.ActiveHours, engine.HourRange{From: int(h.FromHour), To: int(h.ToHour)})
		}
		if d.TrueCharacter != nil {
			p.TrueCharacter = d.TrueCharacter.Description
		}
		out = append(out, p)
	}
	return out, nil
}

func toPersonaDoc(p engine.Persona) personaDoc {
	d := personaDoc{
		ID:             bin(p.ID),
		Handle:         p.Handle,
		DisplayName:    p.DisplayName,
		Type:           p.Type,
		Interests:      interestsDoc{Regions: nonNil(p.Regions), Styles: nonNil(p.Styles)},
		Voice:          voiceDoc{Description: p.VoiceDesc, EmojiRateBP: p.EmojiRateBP, Slang: nonNil(p.Slang)},
		PersonaVersion: engine.PersonaVersion,
	}
	for _, h := range p.ActiveHours {
		d.ActiveHours = append(d.ActiveHours, hoursDoc{FromHour: int32(h.From), ToHour: int32(h.To)})
	}
	if p.TrueCharacter != "" {
		d.TrueCharacter = &trueCharDoc{Description: p.TrueCharacter}
	}
	return d
}

// nonNil makes sure an empty slice is written as an array, not null, so it passes the validator.
func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
