/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::error::Error;

#[derive(Debug, Clone)]
pub struct PriorityItem {
    pub priority: i64,
    pub reader_index: usize,
    pub row_index: usize,
    pub batch_id: usize,
}

impl PartialEq for PriorityItem {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl Eq for PriorityItem {}

impl PartialOrd for PriorityItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriorityItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other.priority.cmp(&self.priority) // Reverse for min-heap
    }
}

struct ReaderState {
    reader: ParquetRecordBatchReader,
    current_batch: Option<RecordBatch>,
    current_row: usize,
    batch_id: usize,
}

pub struct ParquetReaderHeap<F>
where
    F: Fn(&RecordBatch, usize) -> Result<i64, Box<dyn Error>>,
{
    heap: BinaryHeap<PriorityItem>,
    readers: Vec<Option<ReaderState>>,
    priority_fn: F,
}

impl<F> ParquetReaderHeap<F>
where
    F: Fn(&RecordBatch, usize) -> Result<i64, Box<dyn Error>>,
{
    pub fn new(readers: Vec<ParquetRecordBatchReader>, priority_fn: F) -> Result<Self, Box<dyn Error>> {
        let mut heap = BinaryHeap::new();
        let mut reader_states = Vec::new();

        for (index, mut reader) in readers.into_iter().enumerate() {
            if let Some(batch_result) = reader.next() {
                let batch = batch_result?;
                if batch.num_rows() > 0 {
                    let priority = priority_fn(&batch, 0)?;
                    heap.push(PriorityItem {
                        priority,
                        reader_index: index,
                        row_index: 0,
                        batch_id: 0,
                    });
                    reader_states.push(Some(ReaderState {
                        reader,
                        current_batch: Some(batch),
                        current_row: 0,
                        batch_id: 0,
                    }));
                } else {
                    reader_states.push(None);
                }
            } else {
                reader_states.push(None);
            }
        }

        Ok(Self {
            heap,
            readers: reader_states,
            priority_fn,
        })
    }

    pub fn peek(&self) -> Option<(&RecordBatch, &PriorityItem)> {
        if let Some(item) = self.heap.peek() {
            if let Some(Some(reader_state)) = self.readers.get(item.reader_index) {
                if let Some(ref batch) = reader_state.current_batch {
                    return Some((batch, item));
                }
            }
        }
        None
    }

    pub fn pop(&mut self) -> Option<PriorityItem> {
        if let Some(item) = self.heap.pop() {
            let reader_index = item.reader_index;
            let reader_state = self.readers[reader_index].as_mut()?;
            reader_state.current_row += 1;

            if reader_state.current_row < reader_state.current_batch.as_ref()?.num_rows() {
                if let Ok(priority) = (self.priority_fn)(reader_state.current_batch.as_ref()?, reader_state.current_row) {
                    self.heap.push(PriorityItem {
                        priority,
                        reader_index,
                        row_index: reader_state.current_row,
                        batch_id: reader_state.batch_id,
                    });
                }
            } else {
                if let Some(batch_result) = reader_state.reader.next() {
                    if let Ok(new_batch) = batch_result {
                        if new_batch.num_rows() > 0 {
                            if let Ok(priority) = (self.priority_fn)(&new_batch, 0) {
                                reader_state.current_batch = Some(new_batch);
                                reader_state.current_row = 0;
                                reader_state.batch_id += 1;
                                self.heap.push(PriorityItem {
                                    priority,
                                    reader_index,
                                    row_index: 0,
                                    batch_id: reader_state.batch_id,
                                });
                            }
                        } else {
                            self.readers[reader_index] = None;
                        }
                    } else {
                        self.readers[reader_index] = None;
                    }
                } else {
                    self.readers[reader_index] = None;
                }
            }

            return Some(item);
        }
        None
    }

    pub fn pop_with_batch(&mut self) -> Option<(PriorityItem, RecordBatch)> {
        if let Some(item) = self.heap.pop() {
            let reader_index = item.reader_index;
            let reader_state = self.readers[reader_index].as_mut()?;
            let batch = reader_state.current_batch.as_ref()?.clone();

            reader_state.current_row += 1;

            if reader_state.current_row < reader_state.current_batch.as_ref()?.num_rows() {
                if let Ok(priority) = (self.priority_fn)(reader_state.current_batch.as_ref()?, reader_state.current_row) {
                    self.heap.push(PriorityItem {
                        priority,
                        reader_index,
                        row_index: reader_state.current_row,
                        batch_id: reader_state.batch_id,
                    });
                }
            } else {
                if let Some(batch_result) = reader_state.reader.next() {
                    if let Ok(new_batch) = batch_result {
                        if new_batch.num_rows() > 0 {
                            if let Ok(priority) = (self.priority_fn)(&new_batch, 0) {
                                reader_state.current_batch = Some(new_batch);
                                reader_state.current_row = 0;
                                reader_state.batch_id += 1;
                                self.heap.push(PriorityItem {
                                    priority,
                                    reader_index,
                                    row_index: 0,
                                    batch_id: reader_state.batch_id,
                                });
                            }
                        } else {
                            self.readers[reader_index] = None;
                        }
                    } else {
                        self.readers[reader_index] = None;
                    }
                } else {
                    self.readers[reader_index] = None;
                }
            }

            return Some((item, batch));
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}
