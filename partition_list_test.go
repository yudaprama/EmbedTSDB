package embedtsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_partitionList_Remove(t *testing.T) {
	tests := []struct {
		name              string
		partitionList     partitionListImpl
		target            partition
		wantErr           bool
		wantPartitionList partitionListImpl
	}{
		{
			name:          "empty partition",
			partitionList: partitionListImpl{},
			wantErr:       true,
		},
		{
			name: "remove the head node",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			target: &fakePartition{
				minT: 1,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 1,
				head: &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				},
				tail: &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				},
			},
		},
		{
			name: "remove the tail node",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			target: &fakePartition{
				minT: 2,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 1,
				head: &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
				},
				tail: &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
				},
			},
		},
		{
			name: "remove the middle node",
			partitionList: func() partitionListImpl {
				third := &partitionNode{
					val: &fakePartition{
						minT: 3,
					},
				}
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
					next: third,
				}
				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 3,
					head:          first,
					tail:          third,
				}
			}(),
			target: &fakePartition{
				minT: 2,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 2,
				head: &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: &partitionNode{
						val: &fakePartition{
							minT: 3,
						},
					},
				},
				tail: &partitionNode{
					val: &fakePartition{
						minT: 3,
					},
				},
			},
		},
		{
			name: "given node not found",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			target: &fakePartition{
				minT: 3,
			},
			wantPartitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			wantErr: true,
		},
	}
	for i := range tests {
		tc := &tests[i] // use pointer to avoid copying
		t.Run(tc.name, func(t *testing.T) {
			err := tc.partitionList.remove(tc.target)
			assert.Equal(t, tc.wantErr, err != nil)
			// Reset headCache for comparison since it's an internal optimization
			tc.partitionList.headCache = nil
			tc.wantPartitionList.headCache = nil
			assert.Equal(t, &tc.wantPartitionList, &tc.partitionList)
		})
	}
}

func Test_partitionList_Swap(t *testing.T) {
	tests := []struct {
		name              string
		partitionList     partitionListImpl
		old               partition
		new               partition
		wantErr           bool
		wantPartitionList partitionListImpl
	}{
		{
			name:          "empty partition",
			partitionList: partitionListImpl{},
			wantErr:       true,
		},
		{
			name: "swap the head node",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			old: &fakePartition{
				minT: 1,
			},
			new: &fakePartition{
				minT: 100,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 2,
				head: &partitionNode{
					val: &fakePartition{
						minT: 100,
					},
					next: &partitionNode{
						val: &fakePartition{
							minT: 2,
						},
					},
				},
				tail: &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				},
			},
		},
		{
			name: "swap the tail node",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			old: &fakePartition{
				minT: 2,
			},
			new: &fakePartition{
				minT: 100,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 2,
				head: &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: &partitionNode{
						val: &fakePartition{
							minT: 100,
						},
					},
				},
				tail: &partitionNode{
					val: &fakePartition{
						minT: 100,
					},
				},
			},
		},
		{
			name: "swap the middle node",
			partitionList: func() partitionListImpl {
				third := &partitionNode{
					val: &fakePartition{
						minT: 3,
					},
				}
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
					next: third,
				}

				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 3,
					head:          first,
					tail:          third,
				}
			}(),
			old: &fakePartition{
				minT: 2,
			},
			new: &fakePartition{
				minT: 100,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 3,
				head: &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: &partitionNode{
						val: &fakePartition{
							minT: 100,
						},
						next: &partitionNode{
							val: &fakePartition{
								minT: 3,
							},
						},
					},
				},
				tail: &partitionNode{
					val: &fakePartition{
						minT: 3,
					},
				},
			},
		},
		{
			name: "given node not found",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			old: &fakePartition{
				minT: 100,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 2,
				head: &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: &partitionNode{
						val: &fakePartition{
							minT: 2,
						},
					},
				},
				tail: &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				},
			},
			wantErr: true,
		},
	}
	for i := range tests {
		tc := &tests[i] // use pointer to avoid copying
		t.Run(tc.name, func(t *testing.T) {
			err := tc.partitionList.swap(tc.old, tc.new)
			assert.Equal(t, tc.wantErr, err != nil)
			// Reset headCache for comparison since it's an internal optimization
			tc.partitionList.headCache = nil
			tc.wantPartitionList.headCache = nil
			assert.Equal(t, &tc.wantPartitionList, &tc.partitionList)
		})
	}
}
