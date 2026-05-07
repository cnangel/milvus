// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pulsar

import (
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
)

func TestPulsarID_Serialize(t *testing.T) {
	mid := pulsar.EarliestMessageID()
	pid := &pulsarID{
		messageID: mid,
	}

	binary := pid.Serialize()
	assert.NotNil(t, binary)
	assert.NotZero(t, len(binary))
}

func Test_AtEarliestPosition(t *testing.T) {
	mid := pulsar.EarliestMessageID()
	pid := &pulsarID{
		messageID: mid,
	}
	assert.True(t, pid.AtEarliestPosition())

	mid = pulsar.LatestMessageID()
	pid = &pulsarID{
		messageID: mid,
	}
	assert.False(t, pid.AtEarliestPosition())
}

func TestLessOrEqualThan(t *testing.T) {
	msg1 := pulsar.EarliestMessageID()
	pid1 := &pulsarID{
		messageID: msg1,
	}

	msg2 := pulsar.LatestMessageID()
	pid2 := &pulsarID{
		messageID: msg2,
	}

	ret, err := pid1.LessOrEqualThan(pid2.Serialize())
	assert.NoError(t, err)
	assert.True(t, ret)

	ret, err = pid2.LessOrEqualThan(pid1.Serialize())
	assert.NoError(t, err)
	assert.False(t, ret)

	ret, err = pid2.LessOrEqualThan([]byte{1})
	assert.Error(t, err)
	assert.False(t, ret)
}

// TestLessOrEqualThan_Lexicographic verifies that LessOrEqualThan uses
// lexicographic (ledger, entry, batchIdx) ordering, not componentwise AND.
// The old AND-based implementation failed when a smaller ledger was paired
// with a larger entry than the reference ID.
func TestLessOrEqualThan_Lexicographic(t *testing.T) {
	cases := []struct {
		name     string
		pidL     int64
		pidE     int64
		pidB     int32
		refL     int64
		refE     int64
		refB     int32
		expected bool
	}{
		// Cross-ledger: smaller ledger wins regardless of entry
		// Old AND logic: 5<=6 AND 10<=5 → false (WRONG)
		{"smaller_ledger_larger_entry", 5, 10, 0, 6, 5, 0, true},
		// Same ledger, smaller entry wins regardless of batchIdx
		// Old AND logic: 5==5 AND 3<=10 AND 5<=0 → false (WRONG)
		{"same_ledger_smaller_entry_larger_batch", 5, 3, 5, 5, 10, 0, true},
		// Same ledger, same entry, smaller batchIdx
		{"same_ledger_same_entry_smaller_batch", 5, 10, 0, 5, 10, 1, true},
		// Same position (equal)
		{"equal", 5, 10, 0, 5, 10, 0, true},
		// Same ledger, larger entry → false
		{"same_ledger_larger_entry", 5, 10, 0, 5, 9, 0, false},
		// Larger ledger → false
		{"larger_ledger_smaller_entry", 6, 3, 0, 5, 100, 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pid := &pulsarID{messageID: pulsar.NewMessageID(tc.pidL, tc.pidE, tc.pidB, -1)}
			ref := &pulsarID{messageID: pulsar.NewMessageID(tc.refL, tc.refE, tc.refB, -1)}
			ret, err := pid.LessOrEqualThan(ref.Serialize())
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, ret, "case: %s", tc.name)
		})
	}
}

func TestPulsarID_Equal(t *testing.T) {
	msg1 := pulsar.EarliestMessageID()
	pid1 := &pulsarID{
		messageID: msg1,
	}

	msg2 := pulsar.LatestMessageID()
	pid2 := &pulsarID{
		messageID: msg2,
	}

	{
		ret, err := pid1.Equal(pid1.Serialize())
		assert.NoError(t, err)
		assert.True(t, ret)
	}

	{
		ret, err := pid1.Equal(pid2.Serialize())
		assert.NoError(t, err)
		assert.False(t, ret)
	}
}

func Test_SerializePulsarMsgID(t *testing.T) {
	mid := pulsar.EarliestMessageID()

	binary := SerializePulsarMsgID(mid)
	assert.NotNil(t, binary)
	assert.NotZero(t, len(binary))
}

func Test_DeserializePulsarMsgID(t *testing.T) {
	mid := pulsar.EarliestMessageID()

	binary := SerializePulsarMsgID(mid)
	res, err := DeserializePulsarMsgID(binary)
	assert.NoError(t, err)
	assert.NotNil(t, res)
}

func Test_PulsarMsgIDToString(t *testing.T) {
	mid := pulsar.EarliestMessageID()

	str := msgIDToString(mid)
	assert.NotNil(t, str)
	assert.NotZero(t, len(str))
}

func Test_StringToPulsarMsgID(t *testing.T) {
	mid := pulsar.EarliestMessageID()

	str := msgIDToString(mid)
	res, err := stringToMsgID(str)
	assert.NoError(t, err)
	assert.NotNil(t, res)
}
