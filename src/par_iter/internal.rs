//! Internal traits and functions used to implement parallel
//! iteration. These should be considered highly unstable: users of
//! parallel iterators should not need to interact with them directly.
//! See `README.md` for a high-level overview.

use join;
use super::IndexedParallelIterator;
use super::len::*;
use thread_pool::get_registry;

pub trait ProducerCallback<ITEM> {
    type Output;
    fn callback<P>(self, producer: P) -> Self::Output
        where P: Producer<Item=ITEM>;
}

/// A producer which will produce a fixed number of items N. This is
/// not queryable through the API; the consumer is expected to track
/// it.
pub trait Producer: IntoIterator + Send + Sized {
    type Splitter: Splitter; // someday use default = ThiefSplitter

    /// Cost to produce `len` items, where `len` must be `N`.
    fn cost(&mut self, len: usize) -> f64;

    /// Split into two producers; one produces items `0..index`, the
    /// other `index..N`. Index must be less than `N`.
    fn split_at(self, index: usize) -> (Self, Self);
}

/// A consumer which consumes items that are fed to it.
pub trait Consumer<Item>: Send + Sized {
    type Folder: Folder<Item, Result=Self::Result>;
    type Reducer: Reducer<Self::Result>;
    type Result: Send;

    type Splitter: Splitter; // someday use default = ThiefSplitter

    /// If it costs `producer_cost` to produce the items we will
    /// consume, returns cost adjusted to account for consuming them.
    fn cost(&mut self, producer_cost: f64) -> f64;

    /// Divide the consumer into two consumers, one processing items
    /// `0..index` and one processing items from `index..`. Also
    /// produces a reducer that can be used to reduce the results at
    /// the end.
    fn split_at(self, index: usize) -> (Self, Self, Self::Reducer);

    /// Convert the consumer into a folder that can consume items
    /// sequentially, eventually producing a final result.
    fn into_folder(self) -> Self::Folder;

}

pub trait Folder<Item> {
    type Result;

    /// Consume next item and return new sequential state.
    fn consume(self, item: Item) -> Self;

    /// Finish consuming items, produce final result.
    fn complete(self) -> Self::Result;
}

pub trait Reducer<Result> {
    /// Reduce two final results into one; this is executed after a
    /// split.
    fn reduce(self, left: Result, right: Result) -> Result;
}

/// A stateless consumer can be freely copied.
pub trait UnindexedConsumer<ITEM>: Consumer<ITEM> {
    fn split_off(&self) -> Self;
    fn to_reducer(&self) -> Self::Reducer;
}

/// A splitter controls the policy for splitting into smaller work items.
pub trait Splitter: Copy + Send + Sized {
    fn new<P, C>(len: usize, producer: &mut P, consumer: &mut C) -> Self
        where P: Producer, C: Consumer<P::Item>;
    fn try(&mut self) -> bool;
}

/// Classic cost-splitting uses weights to split until below a threshold.
#[derive(Clone, Copy)]
pub struct CostSplitter(f64);

impl Splitter for CostSplitter {
    #[inline]
    fn new<P, C>(len: usize, producer: &mut P, consumer: &mut C) -> Self
        where P: Producer, C: Consumer<P::Item>
    {
        let producer_cost = producer.cost(len);
        let cost = consumer.cost(producer_cost);
        CostSplitter(cost)
    }

    #[inline]
    fn try(&mut self) -> bool {
        if self.0 > THRESHOLD {
            self.0 /= 2.0;
            true
        } else {
            false
        }
    }
}

/// Thief-splitting is an adaptive policy that starts by splitting into enough
/// jobs for every worker thread, and then resets itself whenever a job is
/// actually stolen into a different thread.
#[derive(Clone, Copy)]
pub struct ThiefSplitter {
    origin: usize,
    splits: usize,
}

impl ThiefSplitter {
    #[inline]
    fn id() -> usize {
        // The actual `ID` value is irrelevant.  We're just using its TLS
        // address as a unique thread key, faster than a real thread-id call.
        thread_local!{ static ID: bool = false; }
        ID.with(|id| id as *const bool as usize )
    }
}

impl Splitter for ThiefSplitter {
    #[inline]
    fn new<P, C>(_len: usize, _producer: &mut P, _consumer: &mut C) -> Self
        where P: Producer, C: Consumer<P::Item>
    {
        ThiefSplitter {
            origin: ThiefSplitter::id(),
            splits: get_registry().num_threads(),
        }
    }

    #[inline]
    fn try(&mut self) -> bool {
        let id = ThiefSplitter::id();
        if self.origin != id {
            self.origin = id;
            self.splits = get_registry().num_threads();
            true
        } else if self.splits > 0 {
            self.splits /= 2;
            true
        } else {
            false
        }
    }
}

pub fn bridge<PAR_ITER,C>(mut par_iter: PAR_ITER,
                             consumer: C)
                             -> C::Result
    where PAR_ITER: IndexedParallelIterator, C: Consumer<PAR_ITER::Item>
{
    let len = par_iter.len();
    return par_iter.with_producer(Callback { len: len,
                                             consumer: consumer, });

    struct Callback<C> {
        len: usize,
        consumer: C,
    }

    impl<C, ITEM> ProducerCallback<ITEM> for Callback<C>
        where C: Consumer<ITEM>
    {
        type Output = C::Result;
        fn callback<P>(mut self, mut producer: P) -> C::Result
            where P: Producer<Item=ITEM>
        {
            type S = CostSplitter; // FIXME resolve P::Splitter ∪ C::Splitter
            let splitter = S::new(self.len, &mut producer, &mut self.consumer);
            bridge_producer_consumer(self.len, splitter, producer, self.consumer)
        }
    }
}

fn bridge_producer_consumer<S,P,C>(len: usize,
                                   mut splitter: S,
                                   producer: P,
                                   consumer: C)
                                   -> C::Result
    where S: Splitter, P: Producer, C: Consumer<P::Item>
{
    if len > 1 && splitter.try() {
        let mid = len / 2;
        let (left_producer, right_producer) = producer.split_at(mid);
        let (left_consumer, right_consumer, reducer) = consumer.split_at(mid);
        let (left_result, right_result) =
            join(move || bridge_producer_consumer(mid, splitter,
                                                  left_producer, left_consumer),
                 move || bridge_producer_consumer(len - mid, splitter,
                                                  right_producer, right_consumer));
        reducer.reduce(left_result, right_result)
    } else {
        let mut folder = consumer.into_folder();
        for item in producer {
            folder = folder.consume(item);
        }
        folder.complete()
    }
}

/// Utility type for consumers that don't need a "reduce" step. Just
/// reduces unit to unit.
pub struct NoopReducer;

impl Reducer<()> for NoopReducer {
    fn reduce(self, _left: (), _right: ()) { }
}
