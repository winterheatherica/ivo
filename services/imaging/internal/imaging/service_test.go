package imaging

import (
	"context"
	"testing"

	"github.com/google/uuid"

	imagingv1 "ivo/gen/ivo/imaging/v1"
)

func TestLightingTagsCoverEveryHour(t *testing.T) {
	for h := int32(0); h < 24; h++ {
		if len(lightingTags(h)) == 0 {
			t.Fatalf("hour %d has no lighting tags", h)
		}
	}
	if got := lightingTags(17)[0]; got != "sunset" {
		t.Fatalf("hour 17 = %q, want sunset", got)
	}
}

func TestPlaceholderPassesEveryJob(t *testing.T) {
	jobs := []Job{
		{ID: uuid.New(), Spec: RenderSpec{PositiveTags: []string{"smile"}}},
		{ID: uuid.New(), Spec: RenderSpec{PositiveTags: []string{"night"}}},
	}
	res, err := Placeholder{}.Render(context.Background(), uuid.New(), jobs)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range res {
		if r.JobID != jobs[i].ID || r.Outcome != imagingv1.RenderOutcome_RENDER_OUTCOME_PASSED || r.S3Key == "" {
			t.Fatalf("result %d is wrong: %+v", i, r)
		}
	}
}
