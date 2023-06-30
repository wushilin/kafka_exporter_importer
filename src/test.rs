pub mod iterators;

fn main() {
    println!("Hello, world");

    let chars = String::from("Hello, world");
    for i in chars.as_bytes() {
        println!("{i}");
    }

    let v1 = vec!("a", "d", "g");
    let iter1 = v1.iter();

    let v2 = vec!("a", "e", "z");
    let iter2 = v2.iter();

    let v3 = vec!("m", "q", "t");
    let iter3 = v3.iter();

    let miter: iterators::MultiplexedIterator<std::slice::Iter<'_, &str>> = 
        iterators::MultiplexedIterator::from_iters(vec!(iter1, iter2, iter3),
        |x| -> i32 {
            let mut result = -1;
            let mut selected:Option<&str> = None;
            for (i, ele) in x.iter().enumerate() {
                if result < 0 {
                    result = i as i32;
                    selected = Some(ele);
                } else {
                    if let Some(min) = selected {
                        if min > ele {
                            result = i as i32;
                            selected = Some(ele);
                        }
                    }
                }
            }
            result
        }
    );
    for i in miter {
        println!("{i}");
    }


    
}