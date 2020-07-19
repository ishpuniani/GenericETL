## ETL pipeline for Geowox take-home assignment

### The Problem
The aim of this assignment is to build an ETL pipeline that proceeds to extract data from propertypriceregister.ie, transform it as per Geowox's schema and refresh the prior ingested data.

### The Solution
As a solution, I have built an extensible ETL pipeline which is configuration based.
What this means is, by simply providing a json configuration, we can use different implementations of key systems.
Additionally, the transformer simply requires a mapping file which can take care of basic data transformations.

An example mapping file can be found at `resources/transform_mapping.json` 
<br>Example config can be found at `resources/pprPipeline.json`

To demonstrate extensibility, scalability we can refer to `helpers/readers.py`.
We have two kinds of readers in the file implementing the same reader interface.
This structure makes it easier for developers to extend existing readers/create new readers and use them by simply changing the config.

Another concept used is the Factory pattern to generate different objects based on the config provided. 
Refer: `helpers/factory.py`

`python_json_config` is also used to build configuration objects from the json config provided.

**To Run**:
``` shell script
pip install -r requirements.txt
python3 src/main.py resources/pprPipeline.json
```

#### System Architecture Diagram:
A high-level system architecture:
 
![alt text](resources/arch.png?raw=true "Pipeline Architecture")