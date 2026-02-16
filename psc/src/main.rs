use chrono::NaiveDate;
use chrono::prelude::*;
use csv::Writer;
use reqwest::blocking::ClientBuilder;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
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

    etag: Option<String>,
    kind: Option<String>,
    name: Option<String>,
    notified_on: Option<NaiveDate>,

    address_line_1: Option<String>,
    address_line_2: Option<String>,
    country: Option<String>,
    locality: Option<String>,
    postal_code: Option<String>,
    premises: Option<String>,

    country_registered: Option<String>,
    legal_authority: Option<String>,
    legal_form: Option<String>,
    place_registered: Option<String>,
    registration_number: Option<String>,

    link_self: Option<String>,
    natures_of_control: Option<String>,
}

// Use Option<> for nested fields that are missing
//      TODO Understand more on Option<>
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

    // "psc-snapshot-{utc}_{file_name}.zip" remains on the heap but is owned by fname
    let fname: String = define_partition_fname(part);
    println!("[->] Partition file name: {}", fname);

    // 'zpath' is a reference to a Path that is owned by the download_zip_file function.
    let zpath: &Path = download_zip_file(&fname);
    let txt_fname: String = extract_txt_file_from_zip(zpath);

    let rows: Vec<Company> = read_json_lines_to_vec(&txt_fname);
    let csv_fname = &txt_fname.replace(".txt", ".csv");
    write_vec_to_csv(rows, &csv_fname);
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

fn download_zip_file(zip_file: &str) -> &Path {
    // 'zip_file' is a borrowed reference to a string slice that is owned by 'fname'.
    // -> understand borrowed references more.

    // 'path' is new Path object on the heap.
    //      "Zero-cost" conversion: reinterprets &str as &Path without copying data.
    let path: &Path = Path::new(zip_file);

    // 'url' is a new string on the heap.
    let url: String = format!("{}/{}", BASE_URL, zip_file);
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
    write(&path, zip_bytes).unwrap();
    path
}
// Ownership of 'path' is moved to 'zpath' in main. 'Path' automatically out of scope.
// Everything else goes out of scope and heap memory is deallocated.

fn extract_txt_file_from_zip(zip_path: &Path) -> String {
    // Ownership of 'zpath' is transferred to 'zip_path'. Data is Path type on the heap.
    println!("[->] Extracting zip path: {:?}", zip_path);

    // 'zip_file' is a new File object that owns its data on the heap.
    let zip_file = File::open(zip_path).unwrap();

    // 'zip_archive' is a new mutable ZipArchive object that owns its data on the heap.
    //      Understanding of the Read trait is required to understand why zip_file is valid.
    let mut zip_archive = ZipArchive::new(zip_file).unwrap();

    // 'zip_content' is a new ZipFile object that owns its data on the heap.
    //      .by_index(0) returns the first file in the archive as a ZipFile object.
    //      .by_index requires a mutable self object, so 'archive' must be mutable.
    //      Set as a mutable variable as copy() requires a mutable reference.
    let mut zip_content = zip_archive.by_index(0).unwrap();

    // 'txt_fname' is a new String that owns its data on the heap.
    let txt_fname = zip_content.name().to_string();

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
    // Return file name as string not File type otherwise permission denied issues
    txt_fname
}

fn read_json_lines_to_vec(txt_file: &str) -> Vec<Company> {
    let mut vec: Vec<Company> = Vec::new();
    let tfile = File::open(txt_file).unwrap();
    let reader = BufReader::new(tfile);

    for line_res in reader.lines() {
        // Understand more on temporary strings and borrowing.
        //      [!] line_res.unwrap().trim() results in an error.
        let line = line_res.unwrap();
        let line = line.trim();

        // Type annotations are required when deserializing a json string
        let company: Company = from_str(&line).unwrap();
        vec.push(company);
    }
    vec
}

fn write_vec_to_csv(vec: Vec<Company>, csv_fname: &str) {
    let mut wtr = Writer::from_path(csv_fname).unwrap();

    for row in vec {
        // Rust recognises wtr as a mutable borrow even though we aren't referencing it?
        wtr.serialize(row).unwrap();
    }
    // wtr.flush().unwrap()
}
