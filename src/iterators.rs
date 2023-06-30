pub struct SequentialIterator<I>
    where I:Iterator 
{
    iters: Vec<I>,
    index: usize,
}
impl<I> SequentialIterator<I> 
    where 
        I:Iterator
{
    pub fn from_iters(iters:Vec<I>) -> SequentialIterator<I> {
        return SequentialIterator{iters, index:0};
    }
}

impl<I> Iterator for SequentialIterator<I> 
    where I:Iterator 
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.iters.len() {
            return None;
        }
        let current_ptr = self.iters.get_mut(self.index).unwrap();
        let next = current_ptr.next();
        match next {
            None => {
                self.index += 1;
                return self.next();
            },
            Some(_) => {
                return next;
            }
        }
    }
}
pub struct MultiplexedIterator<I> 
    where I:Iterator
{
    iters: Vec<I>,
    heads: Vec<Option<<I as Iterator>::Item>>,
    end_flag: Vec<bool>,
    selector: fn(&[&<I as Iterator>::Item]) -> i32,
}

impl<I> Iterator for MultiplexedIterator<I> 
    where 
        I:Iterator
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let mut candidates:Vec<&Self::Item> = Vec::with_capacity(self.heads.len());
        let mut indexes = Vec::with_capacity(self.heads.len());
        for i in 0..self.heads.len() {
            if !*self.end_flag.get(i).unwrap() {
                if let Some(val) = self.heads.get(i).unwrap() {
                    candidates.push(val);
                    indexes.push(i);
                }
            }
        }
        if candidates.len() == 0 {
            return None
        }
        let result_idx = (self.selector)(&candidates);
        if result_idx >=0 && (result_idx as usize) < candidates.len() {
            let actual_index = *indexes.get(result_idx as usize).unwrap();
            let out = self.heads.get_mut(actual_index);
            let new_ele = self.iters.get_mut(actual_index).unwrap().next();
            match new_ele {
                None => {
                    let reference = self.end_flag.get_mut(actual_index).unwrap();
                    *reference = true;
                },
                _ => {},
            }
            if let Some(element) = out {
                let replaced_element = std::mem::replace(element, new_ele);
                replaced_element
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<I> MultiplexedIterator<I> 
    where 
        I:Iterator
{
    pub fn from_iters(target:Vec<I>, selector:fn(&[&<I as Iterator>::Item]) -> i32) -> MultiplexedIterator<I> {
        let length = target.len();
        let end_flag:Vec<bool> = Vec::with_capacity(length);
        let mut result = MultiplexedIterator { 
            iters: target, 
            heads:Vec::with_capacity(length), 
            end_flag, 
            selector 
        };
        result.pre_populate();
        return result;
    }

    fn pre_populate(&mut self) {
        for i in 0..self.iters.len() {
            let target = self.iters.get_mut(i).unwrap();
            let the_value = target.next();
            match the_value {
                None => self.end_flag.push(true),
                _ => self.end_flag.push(false)
            }
            self.heads.push(the_value);
        }
    }
}

