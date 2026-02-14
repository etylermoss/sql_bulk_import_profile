use indexmap::IndexMap;
use itertools::{Itertools, Position};
use rustc_hash::FxBuildHasher;
use std::borrow::Borrow;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{BuildHasher, Hash};

pub struct StringMap<T, S = FxBuildHasher, const AVG_VALUE_SIZE: usize = 16> {
    data: String,
    indices: IndexMap<T, usize, S>,
}

impl<T: Borrow<str> + Hash + Eq, S: BuildHasher + Default> StringMap<T, S> {
    /// # Safety
    ///
    /// `indices` must be monotonically increasing valid byte offsets into `data`,
    /// representing the end of each string slice from the previous index (or 0).
    pub unsafe fn new(data: String, indices: IndexMap<T, usize, S>) -> Self {
        StringMap { data, indices }
    }

    #[inline(always)]
    pub fn get(&self, key: &str) -> Option<&str> {
        if let Some((i, _, &v)) = self.indices.get_full(key) {
            let index = if i == 0 { 0..v } else { self.indices[i - 1]..v };

            Some(&self.data[index])
        } else {
            None
        }
    }
}

impl<T, S, K, V, const AVG_VALUE_SIZE: usize> FromIterator<(K, V)>
    for StringMap<T, S, AVG_VALUE_SIZE>
where
    T: Borrow<str> + Hash + Eq,
    S: BuildHasher + Default,
    K: AsRef<str>,
    V: AsRef<str>,
    for<'a> T: From<&'a str>,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let iter = iter.into_iter();

        let (lower, upper) = iter.size_hint();
        let capacity = upper.unwrap_or(lower);

        let mut data = String::with_capacity(capacity * AVG_VALUE_SIZE);
        let mut indices = IndexMap::with_capacity_and_hasher(capacity, S::default());

        for (k, v) in iter {
            if !indices.contains_key(k.as_ref()) {
                data.push_str(v.as_ref());
                indices.insert(T::from(k.as_ref()), data.len());
            }
        }

        Self { data, indices }
    }
}

impl<T, S> Display for StringMap<T, S>
where
    T: Borrow<str> + Hash + Eq,
    S: BuildHasher + Default,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (position, (key, _)) in self.indices.iter().with_position() {
            if matches!(position, Position::Middle | Position::Last) {
                write!(f, ", ")?;
            }

            let key = key.borrow();

            write!(
                f,
                "\"{}\": \"{}\"",
                key,
                self.get(key).expect("key should exist in map")
            )?;
        }

        write!(f, " }}")?;

        Ok(())
    }
}

impl<T: Debug> Debug for StringMap<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("StringMap")
            .field("data", &self.data)
            .field("indices", &self.indices)
            .finish()
    }
}

pub struct StringMapIter<'a, T, S> {
    map: &'a StringMap<T, S>,
    iter: indexmap::map::Iter<'a, T, usize>,
    prev_end: usize,
}

impl<'a, T, S> Iterator for StringMapIter<'a, T, S> {
    type Item = (&'a T, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((key, &end)) = self.iter.next() {
            let start = self.prev_end;

            self.prev_end = end;

            Some((key, &self.map.data[start..end]))
        } else {
            None
        }
    }
}

impl<'a, T, S> IntoIterator for &'a StringMap<T, S> {
    type Item = <StringMapIter<'a, T, S> as Iterator>::Item;
    type IntoIter = StringMapIter<'a, T, S>;

    fn into_iter(self) -> Self::IntoIter {
        StringMapIter {
            map: self,
            iter: self.indices.iter(),
            prev_end: 0,
        }
    }
}

impl<K, V, const N: usize, T, S, const AVG_VALUE_SIZE: usize> From<[(K, V); N]>
    for StringMap<T, S, AVG_VALUE_SIZE>
where
    K: AsRef<str>,
    V: AsRef<str>,
    T: Borrow<str> + Hash + Eq,
    for<'a> T: From<&'a str>,
    S: BuildHasher + Default,
{
    /// Converts a `[(K, V); N]` into a `StringMap<T>`.
    ///
    /// If any entries in the array have equal keys,
    /// all but one of the corresponding values will be dropped.
    fn from(arr: [(K, V); N]) -> Self {
        Self::from_iter(arr)
    }
}
