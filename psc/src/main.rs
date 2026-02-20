// TODO: Way to many .unwrap() methods used -> replace with match
// TODO: Only pull the file from api if it doesn't already exist
use chrono::{NaiveDate, Utc};
use csv::Writer;
use reqwest::blocking::ClientBuilder;
use serde::{Deserialize, Serialize};
use serde_json;
use std::fs::{File, write};
use std::io::{BufRead, BufReader, copy};
use std::path::Path;
use zip::ZipArchive;

const DATE_FORMAT: &str = "%Y-%m-%d";
const BASE_URL: &str = "https://download.companieshouse.gov.uk";

// Create a dedicated struct for writing to csv.
// This ensures a flattened table is being written rather than nested structs.
#[derive(Serialize, Debug)]
struct CompanyCsv {
    company_number: String,
    etag: String,
    kind: String,
    name: String,
    notified_on: String,
    address_line_1: String,
    // address_line_2: String,
    // country: String,
    // locality: String,
    // postal_code: String,
    // premises: String,

    // country_registered: String,
    // legal_authority: String,
    // legal_form: String,
    // place_registered: String,
    // registration_number: String,

    // link_self: String,
    // natures_of_control: String,
}

// Use Option for nested fields that are missing
// Where Option is not used, it implies records can never be missing
#[derive(Serialize, Deserialize, Debug)]
struct Company {
    company_number: String,
    data: Data,
}

#[derive(Serialize, Deserialize, Debug)]
struct Data {
    address: Option<Address>,
    etag: Option<String>,
    identification: Option<Identification>,
    kind: Option<String>,
    links: Option<Links>,
    name: Option<String>,
    natures_of_control: Option<Vec<String>>,
    notified_on: Option<NaiveDate>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Address {
    address_line_1: Option<String>,
    address_line_2: Option<String>,
    country: Option<String>,
    locality: Option<String>,
    postal_code: Option<String>,
    premises: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Identification {
    country_registered: Option<String>,
    legal_authority: Option<String>,
    legal_form: Option<String>,
    place_registered: Option<String>,
    registration_number: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Links {
    #[serde(rename = "self")]
    self_: Option<String>,
}

fn main() {
    // '1of31' is a string literal that lives in the program binary (read-only memory)
    // for the entire duration of the program. It has a 'static' lifetime and cannot be changed.
    // In order for 'part' to "use" the string, it references the memory address
    // to the memory location where the string lives.
    let part: &str = "1of31";

    // 'fname' is a variable stored on the stack
    // The value bound to 'fname' is a String struct that contains:
    //      A pointer to the heap buffer, a length and a capacity
    // The string value itself e.g. "psc-snapshot-{utc}_{file_name}.zip" remains on the heap
    let zip_fname: String = define_partition_fname(part);
    println!("[->] Partition file name: {}", zip_fname);

    // Path is a "zero-cost" conversion: reinterprets &str as &Path without copying data.
    let zip_path: &Path = Path::new(&zip_fname);

    if !check_path_exists(zip_path) {
        download_zip_file(zip_path);
    }

    let txt_fname: String = zip_fname.replace(".zip", ".txt");
    let txt_path: &Path = Path::new(&txt_fname);

    if !check_path_exists(txt_path) {
        extract_txt_from_zip(zip_path, &txt_fname);
    }
    let rows: Vec<Company> = read_json_lines_to_vec(&txt_fname);

    // Temporary solution until I learn more about enums
    for row in rows {
        // .as_ref() does not try to take the value "out" of Option
        // .unwrap() panics because it demands Some(value)
        let address_data = row.data.address.as_ref();

        let address_line_1 = address_data
            // .and_then() returns None if Option is None
            // Otherwise, returns the result from the function (parameter)
            // .clone() can be inefficient. This is temporary
            .and_then(|addr| addr.address_line_1.clone())
            .unwrap_or("No address line 1".to_string());
            println!("{}", address_line_1)
        }

    // let csv_fname = &txt_fname.replace(".txt", ".csv");
    // println!("{}", csv_fname);
    // write_vec_to_csv(rows, &csv_fname);
}

fn print_type_of<T>(_: &T) {
    println!("{}", std::any::type_name::<T>());
}

fn define_partition_fname(partition: &str) -> String {
    // 'utc' lives on the stack. It owns data of String type on the heap.
    // 'utc' accesses the heap data through a pointer (memory address).
    let utc: String = Utc::now().format(DATE_FORMAT).to_string();

    // 'file_name' is a new String on the heap that can be modified.
    let file_name: String = format!("psc-snapshot-{}_{}.zip", utc, partition);
    file_name
}
// Ownership of data is moved from file_name to fname -> file_name goes out of scope and is dropped
// utc goes out of scope. Heap memory for file_name and utc is deallocated.

fn check_path_exists(path: &Path) -> bool {
    path.try_exists().unwrap_or(false)
}

fn download_zip_file(zip_path: &Path) {
    let url: String = format!("{}/{}", BASE_URL, zip_path.to_str().unwrap());
    println!("[->] Generated URL: {}", url);

    // 'builder' is a new ClientBuilder object that owns its data on the heap.
    //      ClientBuilder::new() creates a new ClientBuilder object on the heap.
    //      .user_agent("reqwest") returns a modified ClientBuilder object on the heap
    let builder = ClientBuilder::new().user_agent("reqwest");

    // 'client' is a new Client object that owns its data on the heap.
    //      .build() creates the actual client, then it is unwrapped from Result type.
    let client = builder.build().unwrap();

    // 'zip_bytes' is a vector of bytes on the heap.
    //      client.get(url) creates a new RequestBuilder object on the heap.
    //      .send() sends the request and is unwrapped from Result type.
    //      .bytes() reads the response body as bytes and is unwrapped from Result type.
    let zip_bytes = client.get(url).send().unwrap().bytes().unwrap();
    println!("[->] Downloaded {} bytes", zip_bytes.len());

    // 'zip_bytes' is written to the file at 'path'.
    //      'contents' uses AsRef trait (requires deeper understanding).
    write(zip_path, zip_bytes).unwrap();
}
// Ownership of 'path' is moved to 'zpath' in main. 'Path' automatically out of scope.
// Everything else goes out of scope and heap memory is deallocated.

fn extract_txt_from_zip(zip_path: &Path, txt_fname: &str) {
    // zip_path is an immutable reference
    println!("[->] Extracting zip path: {:?}", zip_path);
    let zip_file: File = File::open(zip_path).unwrap();

    // ZipArchive requires a reader that implements the Read trait
    let mut zip_archive = ZipArchive::new(zip_file).unwrap();

    // 'zip_content' is a new ZipFile object that owns its data on the heap.
    //      .by_index(0) returns the first file in the archive as a ZipFile object.
    //      .by_index requires a mutable self object, so 'archive' must be mutable.
    //      Set as a mutable variable as copy() requires a mutable reference.
    let mut zip_content = zip_archive.by_index(0).unwrap();

    // 'txt_file' is a new File object that owns its data on the heap.
    //      'txt_fname' is passed as a reference to the string object.
    //      Set as a mutable variable as copy() requires a mutable reference.
    //      When using create() file is read-only.
    let mut txt_file = File::create(&txt_fname).unwrap();

    // copy() requires mutable references.
    //      Read & Write traits require a mutable reference for self
    //      --> more understanding required on this.
    copy(&mut zip_content, &mut txt_file).unwrap();
    println!("[->] Extracted file: {}", &txt_fname);
}

fn read_json_lines_to_vec(txt_file: &str) -> Vec<Company> {
    let mut vec: Vec<Company> = Vec::new();
    let tfile = File::open(txt_file).unwrap();
    let reader = BufReader::new(tfile);

    for line_res in reader.lines() {
        // line_res.unwrap().trim() causes a dangling reference
        // line must own the String value before .trim()
        let line = line_res.unwrap();
        let line = line.trim();

        // Type annotations are required when deserializing a json string
        let company: Company = serde_json::from_str(&line).unwrap();
        vec.push(company);
    }
    vec
}

fn transform_rows() {
    println!("Logic here to handle empty records");
}

// fn write_vec_to_csv(vec: Vec<Company>, csv_fname: &str) {
//     let mut wtr = Writer::from_path(csv_fname).unwrap();

//     for row in vec {
//         // .unwrap() transfers ownership of row.data.address (Address)
//         //      To avoid move errors, we unwrap first before further use
//         //      [!] This means row.data.address is moved and no longer available for use
//         let address = row.data.address.unwrap_or(Address {
//             // Temporary default until I implement matching
//             address_line_1: Some("N/A".to_string()),
//             address_line_2: Some("N/A".to_string()),
//             country: Some("N/A".to_string()),
//             locality: Some("N/A".to_string()),
//             postal_code: Some("N/A".to_string()),
//             premises: Some("N/A".to_string()),
//         });

//         let csv_row = CompanyCsv {
//             // Fields require .unwrap() because our deserialized struct has Option types
//             //      but our serialize struct has String types
//             company_number: row.company_number,
//             // [!] row.data (Data) is a struct on the stack - requires further understanding
//             //      String types in the struct are still on the heap
//             etag: row.data.etag.unwrap(),
//             kind: row.data.kind.unwrap(),
//             // .unwrap_or() takes parameter T which is the value inside Option
//             //      Either its Some(value) or None
//             name: row.data.name.unwrap_or("N/A".to_string()),
//             // How would this work with Some(NaiveDate)?
//             notified_on: row.data.notified_on.unwrap_or("N/A".to_string()),
//             address_line_1: address.address_line_1.unwrap_or("N/A".to_string()),
//             // address_line_2: address.address_line_2.unwrap_or("N/A".to_string()),
//             // country: row.data.address.unwrap().country.unwrap(),
//             // locality: row.data.address.unwrap().locality.unwrap(),
//             // postal_code: row.data.address.unwrap().postal_code.unwrap(),
//             // premises: row.data.address.unwrap().premises.unwrap(),
//             // country_registered: row.data.identification.unwrap().country_registered.unwrap(),
//             // legal_authority: row.data.identification.unwrap().legal_authority.unwrap(),
//             // legal_form: row.data.identification.unwrap().legal_form.unwrap(),
//             // place_registered: row.data.identification.unwrap().place_registered.unwrap(),
//             // registration_number: row
//             //     .data
//             //     .identification
//             //     .unwrap()
//             //     .registration_number
//             //     .unwrap(),
//             // link_self: row.data.links.unwrap().self_.unwrap(),
//             // // .map() is a method of an Option enum, it says
//             // //      "If there is a vec inside this Option enum, join the elements by a |"
//             // //      |v| is an example of a closure, it allows you to create functions in line
//             // //      It can also be .map(function_with_join_logic)
//             // natures_of_control: row.data.natures_of_control.map(|v| v.join("|")).unwrap(),
//         };
//         // Rust recognises wtr as a mutable borrow even though we aren't referencing it?
//         wtr.serialize(csv_row).unwrap();
//     }
//     wtr.flush().unwrap()
// }
