# Convector: Conversational Data Transformation Simplified

Convector facilitates the standardization of conversational datasets, making conversational data preparation for NLP tasks efficient and straightforward.

## Installation

Install with pip:
```bash
pip install convector
```

Or install from source:
```bash
git clone https://github.com/teilomillet/convector && cd convector && pip install .
```

## First Use

Running Convector initiates `config.yaml` in your directory, storing your transformation profiles.

## Using Convector

Transform data files with:
```bash
convector process <file_path> [OPTIONS]
```

Use --help for more information and deep dive in the possibilities.

### Profiles

Use `-p` with a profile name to apply specific transformations. New or modified profiles auto-save to `config.yaml`.

### Default Schema

Out-of-the-box schema structure:
```json
{"instruction":"", "input":"", "output":""}
```

### Examples

Process nested data, assuming conversational format, to JSON:
```bash
convector process data.csv -c -f output.json
```

Automatically create/update a profile with additional columns you want to keep:
```bash
convector process data.csv -p new_profile --add-cols 'col1,col2'
```

## Capabilities

- Standardize datasets for model training.
- Convert varied data formats to a unified structure.
- Automate data preparation processes.

Convector serves as a practical tool for data format standardization, offering a suite of options for custom data transformation needs.
