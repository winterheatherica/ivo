package imaging

import (
	"context"

	"github.com/google/uuid"

	imagingv1 "ivo/gen/ivo/imaging/v1"
)

// RenderSpec is stored in image_jobs.render_spec and sent to the GPU worker.
type RenderSpec struct {
	PositiveTags []string       `json:"positive_tags"`
	NegativeTags []string       `json:"negative_tags"`
	Width        int            `json:"width"`
	Height       int            `json:"height"`
	Background   BackgroundSpec `json:"background"`
}

// BackgroundSpec is the background photo with its placement anchor.
type BackgroundSpec struct {
	S3Key           string  `json:"s3_key"`
	FootX           float32 `json:"foot_x"`
	FootY           float32 `json:"foot_y"`
	Scale           float32 `json:"scale"`
	LightAzimuthDeg int32   `json:"light_azimuth_deg"`
}

// Job is one job sent to the renderer.
type Job struct {
	ID   uuid.UUID
	Spec RenderSpec
}

// Result is the render result of one job (see RenderJobResult in gpu.proto).
type Result struct {
	JobID      uuid.UUID
	Outcome    imagingv1.RenderOutcome
	Attempts   int32
	GPUSeconds float64
	NSFWScores map[string]float64
	ImageTags  []string
	S3Key      string
	Error      string
}

// Renderer runs one batch. M2 adds the Modal implementation.
type Renderer interface {
	Render(ctx context.Context, batchID uuid.UUID, jobs []Job) ([]Result, error)
}

// Placeholder doesn't call a GPU. Every job passes with a placeholder image,
// and the prompt tags stand in for the WD Tagger tags.
type Placeholder struct{}

// PlaceholderKey is the S3 key of the M1 placeholder image.
const PlaceholderKey = "placeholders/idol.webp"

// Render returns a PASSED result for every job.
func (Placeholder) Render(_ context.Context, _ uuid.UUID, jobs []Job) ([]Result, error) {
	out := make([]Result, len(jobs))
	for i, j := range jobs {
		tags := append([]string{"placeholder"}, j.Spec.PositiveTags...)
		out[i] = Result{
			JobID:      j.ID,
			Outcome:    imagingv1.RenderOutcome_RENDER_OUTCOME_PASSED,
			Attempts:   1,
			NSFWScores: map[string]float64{"explicit": 0, "questionable": 0, "sensitive": 0},
			ImageTags:  tags,
			S3Key:      PlaceholderKey,
		}
	}
	return out, nil
}
