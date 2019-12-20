use failure::Error;

use std::collections::HashMap;
use std::path::Path;

use quick_xml::Reader;
use quick_xml::events::Event;

fn main() -> Result<(), Error> {
    let tag_counts = count_tags(Path::new("hubber.xml"))?;
    for (tag_name, count) in tag_counts.iter() {
        println!("{}: {}", String::from_utf8_lossy(tag_name), count);
    }
    Ok(())
}

fn count_tags(path: &Path) -> Result<HashMap<Vec<u8>, u32>, Error> {
    let mut xml_reader = Reader::from_file(path)?;
    let mut buf = vec!();
    let mut tag_counts = HashMap::<Vec<u8>, u32>::new();

    loop {
        match xml_reader.read_event(&mut buf) {
            Ok(Event::Start(ref e)) |
            Ok(Event::Empty(ref e)) => {
                let tag_name = e.name();
                match tag_counts.get_mut(tag_name) {
                    Some(count) => {
                        *count += 1;
                    }
                    None => {
                        tag_counts.insert(tag_name.to_vec(), 1);
                    }
                }
            }
            Ok(Event::Eof) => {
                break;
            }
            Err(e) => {
                Err(e)?;
            }
            _ => {}
        }
    }

    Ok(tag_counts)
}
