extern crate csv;

fn main() {
    let mut rdr = csv::Reader::from_file("/Users/dgaff/Desktop/larger_data/larger_data/user_counts/2015-06-16").unwrap();
    for record in rdr.decode() {
        let (user, post_count): (String, usize) = record.unwrap();
        println!("({}): {}", user, post_count);
    }
}