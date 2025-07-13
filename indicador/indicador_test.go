package indicador

import (
	"math"
	"reflect"
	"testing"
)

// TestVwap tests the VWAP calculation
func TestVwap(t *testing.T) {
	closes := []float64{10, 20, 30, 40, 50}
	volumes := []float64{100, 200, 300, 400, 500}
	expected := []float64{10, 16.67, 23.33, 30, 36.67} // (cumPrice * cumVolume) / cumVolume
	result := Vwap(closes, volumes)

	if len(result) != len(closes) {
		t.Errorf("Vwap() length = %d, want %d", len(result), len(closes))
	}
	for i, v := range result {
		if math.Abs(v-expected[i]) > 0.01 {
			t.Errorf("Vwap() index %d = %.2f, want %.2f", i, v, expected[i])
		}
	}

	// Test invalid input
	if result := Vwap(closes[:3], volumes); result != nil {
		t.Errorf("Vwap() with unequal lengths should return nil")
	}
}

// TestRoc tests the Rate of Change calculation
func TestRoc(t *testing.T) {
	closes := []float64{10, 12, 15, 14, 16}
	period := 2
	expected := []float64{0, 0, 50, 16.67, 6.67} // ((close - close[period]) / close[period]) * 100
	result := Roc(period, closes)

	if len(result) != len(closes) {
		t.Errorf("Roc() length = %d, want %d", len(result), len(closes))
	}
	for i, v := range result {
		if math.Abs(v-expected[i]) > 0.01 {
			t.Errorf("Roc() index %d = %.2f, want %.2f", i, v, expected[i])
		}
	}

	// Test insufficient length
	if result := Roc(5, closes[:4]); result != nil {
		t.Errorf("Roc() with insufficient length should return nil")
	}
}

// TestCmo tests the Chande Momentum Oscillator calculation
func TestCmo(t *testing.T) {
	closes := []float64{10, 12, 11, 14, 13}
	period := 3
	expected := []float64{0, 0, 0, 60, 33.33} // (upSum - downSum) / (upSum + downSum) * 100
	result := Cmo(period, closes)

	if len(result) != len(closes) {
		t.Errorf("Cmo() length = %d, want %d", len(result), len(closes))
	}
	for i, v := range result {
		if math.Abs(v-expected[i]) > 0.01 {
			t.Errorf("Cmo() index %d = %.2f, want %.2f", i, v, expected[i])
		}
	}

	// Test insufficient length
	if result := Cmo(5, closes[:4]); result != nil {
		t.Errorf("Cmo() with insufficient length should return nil")
	}
}

// TestAdx tests the Average Directional Index calculation
func TestAdx(t *testing.T) {
	highs := []float64{12, 14, 15, 16, 17}
	lows := []float64{8, 9, 10, 11, 12}
	closes := []float64{10, 12, 13, 14, 15}
	period := 2
	result := Adx(period, highs, lows, closes)

	if len(result) != len(closes) {
		t.Errorf("Adx() length = %d, want %d", len(result), len(closes))
	}

	// Expected values based on manual calculation
	expected := []float64{0, 0, 0, 33.33, 20} // Simplified for test
	for i, v := range result {
		if math.Abs(v-expected[i]) > 0.01 {
			t.Errorf("Adx() index %d = %.2f, want %.2f", i, v, expected[i])
		}
	}

	// Test invalid input
	if result := Adx(period, highs[:3], lows, closes); result != nil {
		t.Errorf("Adx() with unequal lengths should return nil")
	}
}

// TestCci tests the Commodity Channel Index calculation
func TestCci(t *testing.T) {
	highs := []float64{12, 14, 15, 16, 17}
	lows := []float64{8, 9, 10, 11, 12}
	closes := []float64{10, 12, 13, 14, 15}
	period := 3
	result := Cci(period, highs, lows, closes)

	if len(result) != len(closes) {
		t.Errorf("Cci() length = %d, want %d", len(result), len(closes))
	}

	// Basic sanity check: CCI values should be reasonable
	for i, v := range result {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			t.Errorf("Cci() index %d = %.2f, want valid number", i, v)
		}
	}

	// Test invalid input
	if result := Cci(period, highs[:3], lows, closes); result != nil {
		t.Errorf("Cci() with unequal lengths should return nil")
	}
}

// TestPadIndicator tests the padding of indicator arrays
func TestPadIndicator(t *testing.T) {
	ind := []float64{10, 20, 30}
	fullLen := 5
	period := 2
	expected := []float64{0, 0, 10, 20, 30}
	result := PadIndicator(ind, fullLen, period)

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("PadIndicator() = %v, want %v", result, expected)
	}

	// Test empty input
	result = PadIndicator(nil, fullLen, period)
	expected = make([]float64, fullLen)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("PadIndicator() with nil input = %v, want %v", result, expected)
	}
}
