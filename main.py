from extract import extract_properties, locations
from transform import transform
from load import load_data


def run_pipeline():
    print("starting pipeline run")

    for location in locations:
        raw_file = extract_properties(location["city"],location["state"])
        if raw_file:
            clean_file = transform(raw_file)
            load_file = load_file(clean_file)
            print("ETL pipeline successful")
        else:
            print("ETL pipeline Not Successsful")

if __name__ == "__main__":
    run_pipeline()


