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
    name: String,
    kind: String,
    ceased: bool,
    ceased_on: NaiveDate,
    notified_on: NaiveDate,
    title: String,
    forename: String,
    surname: String,
    birth_month: i32,
    birth_year: i32,
    country_of_residence: String,
    address_line_1: String,
    address_line_2: String,
    country: String,
    locality: String,
    postal_code: String,
    premises: String,
    registration_number: String,
    country_registered: String,
    legal_authority: String,
    legal_form: String,
    place_registered: String,
    // natures_of_control: String, // Handle array of strings?
    appointment_verification_statement_date: NaiveDate,
    appointment_verification_statement_due_on: NaiveDate,
    // anti_money_laundering_supervisory_bodies: String, // Handle array of strings?
    etag: String,
    link: String,
}

// Use Option for nested fields that are missing
// Where Option is not used, it implies records can never be missing
// Default allows automatic default values for .unwrap_or_default()
#[derive(Serialize, Deserialize, Debug, Default)]
struct Company {
    company_number: String,
    data: Data,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct Data {
    // Root fields
    name: Option<String>,
    kind: Option<String>,
    etag: Option<String>,
    ceased: Option<bool>,
    ceased_on: Option<NaiveDate>,
    notified_on: Option<NaiveDate>,
    country_of_residence: Option<String>,
    nationality: Option<String>,
    natures_of_control: Option<Vec<String>>,

    // Nested fields
    name_elements: Option<NameElements>,
    date_of_birth: Option<DateOfBirth>,
    address: Option<Address>,
    identification: Option<Identification>,
    verification_details: Option<VerificationDetails>,
    links: Option<Links>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct NameElements {
    forename: Option<String>,
    surname: Option<String>,
    title: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct DateOfBirth {
    month: Option<i32>,
    year: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct Address {
    address_line_1: Option<String>,
    address_line_2: Option<String>,
    country: Option<String>,
    locality: Option<String>,
    postal_code: Option<String>,
    premises: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct Identification {
    country_registered: Option<String>,
    legal_authority: Option<String>,
    legal_form: Option<String>,
    place_registered: Option<String>,
    registration_number: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct VerificationDetails {
    appointment_verification_statement_date: Option<NaiveDate>,
    appointment_verification_statement_due_on: Option<NaiveDate>,
    anti_money_laundering_supervisory_bodies: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct Links {
    #[serde(rename = "self")]
    link: Option<String>,
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
        println!("[->] Extracting contents from zip...");
        extract_txt_from_zip(zip_path, &txt_fname);
    }

    let rows: Vec<Company> = read_json_lines_to_vec(&txt_fname);
    // let transformed_rows: Vec<TransformedCompany> = transform_rows(rows);
    transform_rows(rows);

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
    let utc = Utc::now().format(DATE_FORMAT);
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
    let tfile = File::open(txt_file).expect("Text file does not exist");
    let reader = BufReader::new(tfile);

    // Initiate a Vec struct on the stack (no heap allocation yet).
    let mut companies = Vec::new();

    for line_res in reader.lines() {
        // .trim() returns a string slice (ref to part of the heap data).
        // line_res.unwrap().trim() causes a dangling reference.
        // line must own the String value before .trim() borrows it (topic: lifetimes).
        let line = line_res.unwrap();
        let trimmed = line.trim();

        // Next item in the loop if the line is empty.
        if trimmed.is_empty() {
            continue;
        }

        // Match to skip invalid serializations.
        let company: Company = match serde_json::from_str(trimmed) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Skipping invalid JSON line: {} ({})", trimmed, e);
                continue;
            }
        };
        // Push a Company struct onto the heap.
        companies.push(company);
    }
    companies
}

fn handle_missing_strings(option: Option<String>) -> String {
    match option {
        Some(s) => s,
        None => "No data provided".to_string(),
    }
}

fn handle_missing_dates(option: Option<NaiveDate>) -> NaiveDate {
    match option {
        Some(d) => d,
        None => NaiveDate::default(),
    }
}

fn transform_rows(companies: Vec<Company>) -> Vec<TransformedCompany> {
    // Initiate a Vec struct on the stack.
    let mut transformed_companies = Vec::new();

    // Use vec over &vec (shared reference).
    // Take ownership of vec and move fields out of Company.
    for row in companies {
        // Initialize structs
        // .unwrap_or_default() will take None as the default value
        let dob_data = row.data.date_of_birth.unwrap_or_default();
        let address_data = row.data.address.unwrap_or_default();
        let identity_data = row.data.identification.unwrap_or_default();
        let verify_data = row.data.verification_details.unwrap_or_default();
        let link_data = row.data.links.unwrap_or_default();
        let name_data = row.data.name_elements.unwrap_or_default();

        let transformed_row = TransformedCompany {
            company_number: row.company_number,

            // Root fields
            name: handle_missing_strings(row.data.name),
            kind: handle_missing_strings(row.data.kind),
            ceased: row.data.ceased.unwrap_or_default(),
            ceased_on: handle_missing_dates(row.data.ceased_on),
            notified_on: handle_missing_dates(row.data.notified_on),
            etag: handle_missing_strings(row.data.etag),
            country_of_residence: handle_missing_strings(row.data.country_of_residence),

            // Name fields
            forename: handle_missing_strings(name_data.forename),
            surname: handle_missing_strings(name_data.surname),
            title: handle_missing_strings(name_data.title),

            // DOB fields
            birth_month: dob_data.month.unwrap_or_default(),
            birth_year: dob_data.year.unwrap_or_default(),

            // Address fields
            address_line_1: handle_missing_strings(address_data.address_line_1),
            address_line_2: handle_missing_strings(address_data.address_line_2),
            country: handle_missing_strings(address_data.country),
            locality: handle_missing_strings(address_data.locality),
            postal_code: handle_missing_strings(address_data.postal_code),
            premises: handle_missing_strings(address_data.premises),

            // Identification fields
            country_registered: handle_missing_strings(identity_data.country_registered),
            legal_authority: handle_missing_strings(identity_data.legal_authority),
            legal_form: handle_missing_strings(identity_data.legal_form),
            place_registered: handle_missing_strings(identity_data.place_registered),
            registration_number: handle_missing_strings(identity_data.registration_number),

            // Verification fields
            appointment_verification_statement_date: handle_missing_dates(
                verify_data.appointment_verification_statement_date,
            ),
            appointment_verification_statement_due_on: handle_missing_dates(
                verify_data.appointment_verification_statement_due_on,
            ),

            // Link fields
            link: handle_missing_strings(link_data.link),
        };
        transformed_companies.push(transformed_row);
    }
    transformed_companies
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
