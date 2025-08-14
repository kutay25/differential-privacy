//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package pbeam

import (
	// "fmt"
	// "reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	log "github.com/golang/glog"
	"github.com/google/differential-privacy/go/v4/noise"
	"github.com/google/differential-privacy/privacy-on-beam/v4/internal/kv"
)

func CountNew(s beam.Scope, pcol PrivatePCollection, params CountParams) beam.PCollection {
	s = s.Scope("pbeam.CountNew")

	// TODO: placeholder, move to CountParams
	maxContributions := int64(4)
	// TODO: are these correct?
	params.MaxPartitionsContributed = maxContributions
	params.MaxValue = maxContributions

	// Obtain type information from the underlying PCollection<K,V>.
	idT, partitionT := beam.ValidateKVType(pcol.col)

	// Get privacy parameters.
	spec := pcol.privacySpec
	var err error
	params.AggregationEpsilon, params.AggregationDelta, err = spec.aggregationBudget.consume(params.AggregationEpsilon, params.AggregationDelta)
	if err != nil {
		log.Fatalf("Couldn't consume aggregation budget for Count: %v", err)
	}
	if params.PublicPartitions == nil {
		params.PartitionSelectionParams.Epsilon, params.PartitionSelectionParams.Delta, err = spec.partitionSelectionBudget.consume(params.PartitionSelectionParams.Epsilon, params.PartitionSelectionParams.Delta)
		if err != nil {
			log.Fatalf("Couldn't consume partition selection budget for Count: %v", err)
		}
	}

	var noiseKind noise.Kind
	if params.NoiseKind == nil {
		noiseKind = noise.LaplaceNoise
		log.Infof("No NoiseKind specified, using Laplace Noise by default.")
	} else {
		noiseKind = params.NoiseKind.toNoiseKind()
	}

	err = checkCountParams(params, noiseKind, partitionT.Type())
	if err != nil {
		log.Fatalf("pbeam.Count: %v", err)
	}

	// Drop non-public partitions, if public partitions are specified.
	pcol.col, err = dropNonPublicPartitions(s, pcol, params.PublicPartitions, partitionT.Type())
	if err != nil {
		log.Fatalf("Couldn't drop non-public partitions for Count: %v", err)
	}

	// [0, A] [0, B] [0, B] [0, B] [0, B] -> A: 1, B: 4
	// MaxPartitionsContributed = 2
	// MaxValue = 2
	// MaxContributions = 4
	// WITH MPC/MCPP:			A: 1, B: 2 (loss=2)
	// WITH MC:					A: 1, B: 3 (loss=1)

	// First, contribution bounding (sampling per pid)
	bounded := boundContributions(s, pcol.col, maxContributions) // (pid, pkey)

	// Second, encode KV pairs and count
	coded := beam.ParDo(s, kv.NewEncodeFn(idT, partitionT), bounded) // ((pid,pkey))
	kvCounts := stats.Count(s, coded)                                // ((pid, pkey), count)
	counts64 := beam.ParDo(s, convertToInt64Fn, kvCounts)            // ((pid, pkey), count)
	rekeyed := beam.ParDo(s, rekeyInt64, counts64)                   // (pid, (pkey, count))

	// Third, remove the privacy keys, decode the value, and sum all the counts bounded by MaxValue.
	countPairs := beam.DropKey(s, rekeyed) // ((pkey, count))
	countsKV := beam.ParDo(s,
		newDecodePairInt64Fn(partitionT.Type()),
		countPairs,
		beam.TypeDefinition{Var: beam.WType, T: partitionT.Type()}) // (pkey, count)

	var result beam.PCollection
	// Add public partitions and compute the aggregation output, if public partitions are specified.
	if params.PublicPartitions != nil {
		result = addPublicPartitionsForCount(s, *spec, params, noiseKind, countsKV)
	} else {
		boundedSumFn, err := newBoundedSumInt64Fn(*spec, countToSumParams(params), noiseKind, false)
		if err != nil {
			log.Fatalf("Couldn't get boundedSumInt64Fn for Count: %v", err)
		}
		sums := beam.CombinePerKey(s,
			boundedSumFn,
			countsKV)
		// Drop thresholded partitions.
		result = beam.ParDo(s, dropThresholdedPartitionsInt64, sums)
	}

	if !params.AllowNegativeOutputs {
		// Clamp negative counts to zero.
		result = beam.ParDo(s, clampNegativePartitionsInt64, result)
	}

	return result
}
