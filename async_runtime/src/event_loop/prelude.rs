pub trait VecExt<T> {
    fn retain_filter<F>(&mut self, f: F) -> Vec<T>
    where
        F: FnMut(&T) -> bool;
}

impl<T> VecExt<T> for Vec<T> {
    fn retain_filter<F>(&mut self, mut f: F) -> Vec<T>
    where
        F: FnMut(&T) -> bool,
    {
        let mut filtered: Vec<T> = Vec::new();
        for i in (0..self.len()).into_iter().rev().collect::<Vec<usize>>() {
            if f(&self[i]) {
                filtered.push(self.swap_remove(i));
            }
        }
        filtered
    }
}
