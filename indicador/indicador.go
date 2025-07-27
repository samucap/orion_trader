package indicador

import (
	"fmt"
	"math"

	"github.com/cinar/indicator"
)

// FormatFloat formats a float64 to a string with 2 decimal places or empty string for 0
func FormatFloat(f float64) string {
	if f == 0.0 {
		return ""
	}
	return fmt.Sprintf("%.2f", f)
}

// Vwap calculates Volume Weighted Average Price
func Vwap(closes, volumes []float64) []float64 {
	if len(closes) != len(volumes) {
		return nil
	}
	vwap := make([]float64, len(closes))
	var cumVolume, cumPriceVolume float64
	for i := 0; i < len(closes); i++ {
		cumPriceVolume += closes[i] * volumes[i]
		cumVolume += volumes[i]
		if cumVolume == 0 {
			vwap[i] = 0
		} else {
			vwap[i] = cumPriceVolume / cumVolume
		}
	}
	return vwap
}

// Roc calculates Rate of Change
func Roc(period int, closes []float64) []float64 {
	if len(closes) < period {
		return nil
	}
	roc := make([]float64, len(closes))
	for i := period; i < len(closes); i++ {
		if closes[i-period] == 0 {
			roc[i] = 0
		} else {
			roc[i] = (closes[i] - closes[i-period]) / closes[i-period] * 100
		}
	}
	return roc
}

// Cmo calculates Chande Momentum Oscillator
func Cmo(period int, closes []float64) []float64 {
	if len(closes) < period {
		return nil
	}
	cmo := make([]float64, len(closes))
	for i := period; i < len(closes); i++ {
		var upSum, downSum float64
		for j := i - period + 1; j <= i; j++ {
			change := closes[j] - closes[j-1]
			if change > 0 {
				upSum += change
			} else {
				downSum += math.Abs(change)
			}
		}
		if upSum+downSum == 0 {
			cmo[i] = 0
		} else {
			cmo[i] = (upSum - downSum) / (upSum + downSum) * 100
		}
	}
	return cmo
}

// Adx calculates Average Directional Index
func Adx(period int, highs, lows, closes []float64) []float64 {
	if len(highs) != len(lows) || len(lows) != len(closes) || len(closes) < period+1 {
		return nil
	}
	adx := make([]float64, len(closes))
	tr := make([]float64, len(closes))
	plusDM := make([]float64, len(closes))
	minusDM := make([]float64, len(closes))
	for i := 1; i < len(closes); i++ {
		dh := highs[i] - highs[i-1]
		dl := lows[i-1] - lows[i]
		plusDM[i] = math.Max(dh, 0)
		minusDM[i] = math.Max(dl, 0)
		if dh > dl {
			minusDM[i] = 0
		} else if dl > dh {
			plusDM[i] = 0
		}
		tr[i] = math.Max(highs[i]-lows[i], math.Max(math.Abs(highs[i]-closes[i-1]), math.Abs(lows[i]-closes[i-1])))
	}
	plusDI := indicator.Ema(period, indicator.Sma(period, plusDM))
	minusDI := indicator.Ema(period, indicator.Sma(period, minusDM))
	atr := indicator.Ema(period, indicator.Sma(period, tr))
	for i := period; i < len(closes); i++ {
		if plusDI[i]+minusDI[i] == 0 || atr[i] == 0 {
			adx[i] = 0
		} else {
			dx := math.Abs(plusDI[i]-minusDI[i]) / (plusDI[i]+minusDI[i]) * 100
			adx[i] = dx
		}
	}
	return indicator.Ema(period, adx[period:])
}

// Cci calculates Commodity Channel Index
func Cci(period int, highs, lows, closes []float64) []float64 {
	if len(highs) != len(lows) || len(lows) != len(closes) || len(closes) < period {
		return nil
	}
	cci := make([]float64, len(closes))
	typPrices := make([]float64, len(closes))
	for i := 0; i < len(closes); i++ {
		typPrices[i] = (highs[i] + lows[i] + closes[i]) / 3
	}
	smaTyp := indicator.Sma(period, typPrices)
	for i := period - 1; i < len(closes); i++ {
		meanDev := 0.0
		for j := i - period + 1; j <= i; j++ {
			meanDev += math.Abs(typPrices[j] - smaTyp[i])
		}
		meanDev /= float64(period)
		if meanDev == 0 {
			cci[i] = 0
		} else {
			cci[i] = (typPrices[i] - smaTyp[i]) / (0.015 * meanDev)
		}
	}
	return cci
}

// PadIndicator pads an indicator array to match the input length
func PadIndicator(ind []float64, fullLen, period int) []float64 {
	if len(ind) == 0 {
		return make([]float64, fullLen)
	}
	padded := make([]float64, fullLen)
	copy(padded[fullLen-len(ind):], ind)
	return padded
}
