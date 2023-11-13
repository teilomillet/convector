
# Convector

## Introduction
**Convector** is a tool designed to facilitate the unification of conversational datasets into a consistent format. Capable of handling various dataset formats including JSONL, Parquet, Zstandard (Zst), JSON.GZ, CSV, and TXT, **Convector** converts them into JSONL format. The user can choose to filter data during transformation for enhanced customization. It offers flexibility in output formats with options like the default format and chat_completion format, the latter being compliant with OpenAI's format.

## Installation
**Convector** can be installed either via PyPI or directly from GitHub.

- **Using PyPI**: Run `pip install convector` in the terminal.
- **Using GitHub**: Clone and install using the following commands:
  ```bash
  git clone https://github.com/teilomillet/convector
  cd convector
  pip install .
  ```

## Usage
**Convector** provides a command-line interface for easy data processing with various customization options.

- **Basic Command**: `convector process <file_path> [OPTIONS]`
- **Options**:
  - `-p, --profile`: Predefined profile from the config (default is 'default').
  - `-c, --conversation`: Allow to process conversational exchanges.
  - `--instruction`: Key for instructions or system messages.
  - `-i, --input-key`: Key for user inputs.
  - `-o, --output-key`: Key for bot responses.
  - `-s, --schema`: Schema of the output data.
  - `--filter`: Filter conditions in "field,operator,value" format.
  - `-l, --limit`: Limit to a number of lines.
  - `--bytes`: Limit to a number of bytes.
  - `-f, --file-out`: File for transformed data.
  - `-d, --dir-out`: Directory for output files.
  - `-v, --verbose`: Enable detailed logs.

- **Example Commands**: 
  ---------------------------------------
  - Process each file in a folder:
    ```bash
    convector process /path/to/data/
    ```
  ---------------------------------------
  - Process the file `data.jsonl`, which is a conversation `-c`, keep all the data with an `id` under 10500, the output will be saved in `/path/to/output_dir/output.jsonl`:
    ```bash
    convector process /data.jsonl -c --filter id<10500 -f output.jsonl -d /output_dir/
    ```
  ---------------------------------------
  - Process the file `data.parquet` and output the data into a `chat_completion` format with the `id` and `user_id` at each row. (the output data will be saved in `data_tr.jsonl` inside the default output location (convector/silo)):
    ```bash
    convector process /data.parquet --filter "id;user_id" --schema chat_completion
    ```
  ---------------------------------------
  - Register a profile name `sampler`, process `333` lines of the file `data.parquet` and save the output into a `chat_completion` format in a file name `sampler.jsonl`:
    ```bash
    convector process /data.json -p sampler -l 333 -s chat_completion -f sampler.jsonl
    ```
  ---------------------------------------
  - Process all the files in the folder `/data`, using all the commands previously saved in the profile `sampler` (see above):
    ```bash
    convector process /data/ -p sampler
    ```
  ---------------------------------------

## Advanced Features
- **Conversational Data Handling**: **Convector** efficiently processes nested conversational data. Using the `--conversation` command, it can identify and handle complex conversation structures, auto-generating a `conversation_id` when needed.
- **Customization**: Users can customize the data fields to be retained during processing with the `--filter` option. By default, **Convector** keeps `instruction`, `input`, and `output`. Additional fields can be included as required.
- **Folder Handling**: **Convector** can go through folders to process the data inside it. It will by default, create a file using `_tr` at the end if no `--file-out` is specified.

## Configuration and Customization
- **Profile Customization**: Users can define and use custom profiles for different types of data processing tasks inside the `config.yaml`. The profile will automatically be saved and updated if used with new commands.
- **Schema Application**: **Convector** allows for the application of custom schemas to tailor the output according to specific requirements. 
  - Default Schema:
    ```json
    {"instruction":"","input":"","output":"","source":""}
    ```
  - Chat_completion Schema:
    ```json
    "messages": [
        {"role": "system", "content": ""},
        {"role": "user", "content": ""},
        {"role": "assistant", "content": ""}
    ],
    "source":""
    ```
