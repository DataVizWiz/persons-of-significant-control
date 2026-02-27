// NOTES:
// If we don't want to write to data, use references
use chrono::{NaiveDate, Utc};
use csv::Writer;
use serde::{Deserialize, Serialize};
use serde_json;
use std::fs::{File, write};
use std::io::{BufRead, BufReader, copy};
use std::path::Path;
use tokio;
use zip::ZipArchive;

const DATE_FORMAT: &str = "%Y-%m-%d";
const BASE_URL: &str = "https://download.companieshouse.gov.uk";

// Create a dedicated struct for writing to csv.
// This ensures a flattened table is being written rather than nested structs.
#[derive(Serialize, Debug)]
struct TransformedCompany {
    company_number: String,
    etag: String,
    kind: String,
    name: String,
    notified_on: NaiveDate,
    address_line_1: String,
    address_line_2: String,
    country: String,
    locality: String,
    postal_code: String,
    premises: String,
    // country_registered: String,
    // legal_authority: String,
    // legal_form: String,
    // place_registered: String,
    // registration_number: String,
    // link_self: String,
    // natures_of_control: String,
}

// impl TransformedCompany {
//     fn with_default(&self) {

//     }
// }

// Use Option for nested fields that are missing
// Where Option is not used, it implies records can never be missing
#[derive(Serialize, Deserialize, Debug)]
struct Company {
    company_number: String,
    data: Data,
}

// impl Company {
//     fn handle_missing_strings(self, option_str: Option<String>, default: &str) -> String {
//         option_str.unwrap_or(default.to_string())
//     }
// }

#[derive(Serialize, Deserialize, Debug)]
struct Data {
    address: Option<Address>,
    ceased_on: Option<NaiveDate>,
    country_of_residence: Option<String>,
    date_of_birth: Option<DateOfBirth>,
    etag: Option<String>,
    identification: Option<Identification>,
    verification_details: Option<VerificationDetails>,
    kind: Option<String>,
    links: Option<Links>,
    name: Option<String>,
    name_elements: Option<NameElements>,
    nationality: Option<String>,
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
struct DateOfBirth {
    month: Option<i32>,
    year: Option<i32>,
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
struct VerificationDetails {
    appointment_verification_statement_date: Option<NaiveDate>,
    appointment_verification_statement_due_on: Option<NaiveDate>,
    anti_money_laundering_supervisory_bodies: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Links {
    #[serde(rename = "self")]
    self_: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct NameElements {
    forename: Option<String>,
    surname: Option<String>,
    title: Option<String>,
}

#[tokio::main]
async fn main() {
    // '1of31' is a string literal that lives in the program binary (read-only memory)
    // for the entire duration of the program. It has a 'static' lifetime and cannot be changed.
    // In order for 'part' to "use" the string, it references the memory address
    // to the memory location where the string lives.
    let part: &str = "1of31";

    let zip_fname = define_partition_fname(part);
    println!("[->] Partition file name: {}", zip_fname);

    // &Path::new() points to the same memory location as zip_fname
    //      returns a Path ref
    let zip_path: &Path = Path::new(&zip_fname);
    let exists = zip_path.exists();

    if !exists {
        println!("[->] Downloading zip from url...");
        download_zip_file(zip_path).await;
    }

    let txt_fname = zip_fname.replace(".zip", ".txt");

    if !exists {
        extract_txt_from_zip(zip_path, &txt_fname);
    }

    // let rows: Vec<Company> = read_json_lines_to_vec(&txt_fname);
    // // let transformed_rows: Vec<TransformedCompany> = transform_rows(rows);
    // transform_rows(rows);

    // let csv_fname = &txt_fname.replace(".txt", ".csv");
    // println!("{}", csv_fname);
    // write_vec_to_csv(rows, &csv_fname);
}

fn print_type_of<T>(_: &T) {
    println!("{}", std::any::type_name::<T>());
}

fn define_partition_fname(partition: &str) -> String {
    // 'utc' is a variable allocated to the stack.
    // Utc::now() returns a DateTime struct allocated to the stack.
    // .format() returns a DelayedFormat struct on the stack.
    // .to_string() binds a String struct to utc (also on the stack).
    // String contains a pointer to the text data allocated to the heap.
    let utc: String = Utc::now().format(DATE_FORMAT).to_string();
    format!("psc-snapshot-{}_{}.zip", utc, partition)
}

async fn download_zip_file(zip_path: &Path) {
    // .unwrap() is acceptable here because .to_str() will always return Some(&str).
    let url: String = format!("{}/{}", BASE_URL, zip_path.to_str().unwrap());
    println!("[->] Generated URL: {}", url);

    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .send()
        .await
        .expect("Error making GET request");

    let bytes = response
        .bytes()
        .await
        .expect("Error reading response into bytes");
    println!("[->] Downloaded {} bytes", bytes.len());

    write(zip_path, bytes).expect("Error writing to zip path");
}

fn extract_txt_from_zip(zip_path: &Path, txt_fname: &str) {
    println!("[->] Extracting zip path: {:?}", zip_path);
    let zip_file: File = File::open(zip_path).unwrap();

    // ZipArchive requires a reader that implements the Read trait
    let mut zip_archive = ZipArchive::new(zip_file).unwrap();

    // 'zip_content' is a new ZipFile object that owns its data on the heap.
    //      .by_index(0) returns the first file in the archive as a ZipFile object.
    //      .by_index requires a mutable self object, so 'archive' must be mutable.
    //      Set as a mutable variable as copy() requires a mutable reference.
    let mut zip_content = zip_archive
        .by_index(0)
        .expect("Error getting index 0 from zip file");

    // 'txt_file' is a new File object that owns its data on the heap.
    //      'txt_fname' is passed as a reference to the string object.
    //      Set as a mutable variable as copy() requires a mutable reference.
    //      When using create() file is read-only.
    let mut txt_file = File::create(&txt_fname).unwrap();

    // copy() requires mutable references.
    //      Read & Write traits require a mutable reference for self
    //      --> more understanding required on this.
    copy(&mut zip_content, &mut txt_file).expect("Error writing zip contents to text file");
    println!("[->] Extracted file: {}", &txt_fname);
}

fn read_json_lines_to_vec(txt_file: &str) -> Vec<Company> {
    let tfile = File::open(txt_file).unwrap();
    let reader = BufReader::new(tfile);

    // Initiate a Vec struct on the stack (no heap allocation yet).
    let mut vec: Vec<Company> = Vec::new();

    for line_res in reader.lines() {
        // line_res.unwrap().trim() causes a dangling reference
        // line must own the String value before .trim()
        let line = line_res.unwrap();
        let line = line.trim();

        let company: Company = serde_json::from_str(&line).unwrap();

        // Push a Company struct onto the heap.
        vec.push(company);
    }
    vec
}

fn handle_missing_strings(option_str: Option<String>, default: &str) -> String {
    option_str.unwrap_or(default.to_string())
}

fn transform_rows(vec: Vec<Company>) -> Vec<TransformedCompany> {
    // Initiate a Vec struct on the stack.
    let mut transformed_vec: Vec<TransformedCompany> = Vec::new();

    // Use vec over &vec (shared reference).
    // Take ownership of vec and move fields out of Company.
    for row in vec {
        // Handle missing strings
        let etag = handle_missing_strings(row.data.etag, "No etag");
        let kind = handle_missing_strings(row.data.kind, "No kind");
        let name = handle_missing_strings(row.data.name, "No name");

        // Need a more efficient way to handle defaults
        let address_data = row.data.address.unwrap_or(Address {
            address_line_1: Some("No address line 1".to_string()),
            address_line_2: Some("No address line 2".to_string()),
            country: Some("No country".to_string()),
            locality: Some("No locality".to_string()),
            postal_code: Some("No postal code".to_string()),
            premises: Some("No premises".to_string()),
        });

        let transformed_row = TransformedCompany {
            company_number: row.company_number,
            etag: etag,
            kind: kind,
            name: name,
            notified_on: row.data.notified_on.unwrap_or(NaiveDate::default()),
            address_line_1: address_data
                .address_line_1
                .unwrap_or("No address line 1".to_string()),
            address_line_2: address_data
                .address_line_2
                .unwrap_or("No address line 2".to_string()),
            country: address_data.country.unwrap_or("No country".to_string()),
            locality: address_data.locality.unwrap_or("No locality".to_string()),
            postal_code: address_data
                .postal_code
                .unwrap_or("No postal code".to_string()),
            premises: address_data.premises.unwrap_or("No premises".to_string()),
        };
        println!("{:?}", transformed_row);
        transformed_vec.push(transformed_row);
    }
    transformed_vec
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
