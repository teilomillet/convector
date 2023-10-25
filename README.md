# Convector: Your Data Transportation Wizard 🚂

**Convector** simplifies the process of transforming and standardizing conversational datasets. 
It aim to consolidate and utilize conversational data effectively, enabling a harmonized approach towards data analysis, model training or fine-tuning, and various natural language processing (NLP). applications.

## Core Utility

**Convector** focuses on automatically recognizing and adapting various conversational data structures into a unified, consistent format, essential for users who manage or engage with multiple conversational datasets.

## Features

- **Automated Recognition**: Adept at recognizing and adapting to various conversational data structures.
- **Flexibility**: Customizable keys for a more personalized data handling experience.
- **Command-Line Operability**: Designed for easy operation directly from the command line.
- **Broad Compatibility**: Fluent in JSON, JSONL, CSV, Parquet, and more.
- **Performance Tuned**: Efficient and effective, ensuring your data’s journey is swift and smooth.

## 🚉 Installation 


### 🚶‍♂️ Walkthrough for the Installation

**Clone the Repository:**

- Go to the repertory of your choice (where you want the code to be saved)

```bash
cd PATH/OF/YOUR/CHOICE/
```
- Download the code using git clone

```bash
git clone https://github.com/teilomillet/convector
```

- Navigate to the Cloned Directory:

```bash
cd convector
```
- Install Using Pip:

```bash
pip install .
```

- You can use convector directly from the command line

```bash
convector "PATH/TO/YOUR/DATASET"
```

## 🌐 Universal Ticket - CLI Access

Convector’s CLI simplifies your interaction, making data transformation as easy as a command away.

### Auto-Magic Transformation
Convector’s intelligent auto-recognition will magically transform your data to a unified format.

```bash
convector data.json
```

### Custom Key Express
Tailor your journey by specifying custom keys that suit your unique dataset.

```bash
convector data.jsonl --input='user_message' --output='bot_message' --instruction='system_prompt' --add='id'
```

### Advanced Toolkit
Explore advanced features like byte and lines limitations.

```bash
convector "PATH/TO/data.parquet" --bytes=10000 
```

```bash
convector "PATH/TO/data.csv" --lines=333 --output_file=dataset.jsonl
```

## Python usage 

Or you can operate **Convector** in python, using the following code.

```python
import convector
convector.transform( filepath )
```

## Delivery

The transformed datasets are saved in a standardized JSONL format, ensuring consistency and compatibility for various applications. If no output_file is set, it will be saved under the same name + "_tr" at the end by default.

If there is no instruction equivalent in the dataset, an instruction with a "" value will be set.

``` json
{"instruction": "...","input": "...","output": "..."}
```
### Use Cases

Convector is beneficial for various applications such as:

**Data Consolidation**: Merging and utilizing multiple conversational datasets for comprehensive analysis.
**Model Training**: Preparing datasets effectively for training conversational models.
**Research and Development**: Facilitating a consistent data format for research and application development in NLP.


## Conclusion

Convector offers a reliable and efficient solution to **handle conversational data transformation** needs, ensuring that your data is consistent, usable, and ready for a multitude of applications.