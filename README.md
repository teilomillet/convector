# Convector: Process Your Conversational Datasets 

**Convector** simplifies the process of transforming and standardizing conversational datasets. 
It aim to consolidate and utilize conversational data effectively, enabling a harmonized approach towards data analysis, model training or fine-tuning, and various natural language processing (NLP). applications.

## Core Utility

**Convector** focuses on automatically recognizing and adapting various conversational data structures into a unified, consistent format, essential for users who manage or engage with multiple conversational datasets.

### Use Cases

Convector is beneficial for various applications such as:

**Data Consolidation**: Merging and utilizing multiple conversational datasets for comprehensive analysis.
**Model Training**: Preparing datasets effectively for training and fine-tuning conversational models.
**Research and Development**: Facilitating a consistent data format for research and application development in NLP.

## Features

- **Automated Recognition**: Adept at recognizing and adapting to various conversational data structures.
- **Flexibility**: Customizable keys for a more personalized data handling experience.
- **Command-Line Operability**: Designed for easy operation directly from the command line.
- **Broad Compatibility**: Fluent in JSON, JSONL, CSV, Parquet, and NZST.
- **Performance Tuned**: Efficient and effective, ensuring your data’s process is swift and smooth.

## 🚶‍♂️ Installation 

- This suppose you have python3 installed with pip.

**Open a terminal:**

- Type :

```bash
git clone https://github.com/teilomillet/convector
cd convector
pip install .
```
- You can use convector directly from the command line.

```bash
convector --help
```

## 🌐 Universal Ticket - CLI Access

Convector’s CLI simplifies your interaction, making data processing as easy as a command away.

### Auto-Process
Convector’s intelligent auto-recognition will process your data to a unified format.

```bash
convector process file/path/data.*
```

### Custom Key Express
Tailor your process by specifying custom keys that suit your unique dataset.

Example :
This will process a jsonl file with specifique columns names and transform it in the unified format and keeping the 'id' column.

```bash
convector process data.jsonl --input='user_message' --output='bot_message' --instruction='system_prompt' --add='id'
```

### Advanced Toolkit
Explore advanced features like byte and lines limitations.

```bash
convector process "PATH/TO/data.parquet" --bytes=10000 
```

By setting a '--output_file=' we ensure that the dataset is saved with the name specify.

```bash
convector process "PATH/TO/data.csv" --lines=333 --output_file=dataset.jsonl
```

## Python usage 

Or you can operate **Convector** in python, using the following code.

```python
import convector
convector.process( filepath )
```

## Delivery

The transformed datasets are saved in a standardized JSONL format, ensuring consistency and compatibility for various applications. If no output_file is set, it will be saved under the same name adding "_tr" at the end by default.

If there is no instruction equivalent in the dataset, an instruction with a "" value will be set.

Default :

``` json
{"instruction": "...","input": "...","output": "..."}
```

## Conclusion

Convector offers a reliable and efficient solution to **handle conversational data transformation** needs, ensuring that your data is consistent, usable, and ready for a multitude of applications.