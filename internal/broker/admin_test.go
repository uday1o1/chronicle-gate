package broker

import "testing"

func TestValidateRewindBounds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		requested int64
		start     int64
		end       int64
		wantError bool
	}{
		{name: "record in range", requested: 4, start: 4, end: 5},
		{name: "below start", requested: 3, start: 4, end: 5, wantError: true},
		{name: "at end", requested: 5, start: 4, end: 5, wantError: true},
		{name: "invalid bounds", requested: 4, start: 5, end: 4, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateRewindBounds(test.requested, test.start, test.end)
			if (err != nil) != test.wantError {
				t.Fatalf("ValidateRewindBounds() error = %v, wantError %v", err, test.wantError)
			}
		})
	}
}

func TestValidateEmptyGroupInitialization(t *testing.T) {
	t.Parallel()
	if err := ValidateEmptyGroupInitialization(0, 0); err != nil {
		t.Fatalf("empty topic rejected: %v", err)
	}
	for _, bounds := range [][2]int64{{0, 1}, {1, 1}, {-1, 0}} {
		if err := ValidateEmptyGroupInitialization(bounds[0], bounds[1]); err == nil {
			t.Fatalf("nonempty or invalid bounds %v accepted", bounds)
		}
	}
}
