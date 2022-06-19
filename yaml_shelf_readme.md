Design notes

Preql - An interpreted relational query language that compiles to SQL
https://github.com/erezsh/Preql

https://github.com/googleapis/python-bigquery-sqlalchemy


Shelves can be defined in YAML.


## How does this differ from recipe

* Invariant: You build and run the recipe once
* Constants: Expressions can use a constant that are evaluated in a different context
* Expressions: We use expressions that calculate
* Late binding: Evaluate the expression at the last possible time
* Integrated formats?


department:
    field: department
patient:
    field: "Patient: " + patient_name
    id_field: patient_id
patients:
    field: count([patient])
patients_pct_total:
    # As a pct of global count
    field: count([patient]) / {{count(patient)}}
patients_pct_local:
    # As a pct of local
    field: count([patient]) / {{count(patient)}}
top_departments:
    field: {{bucket}}



Recipe


self.recipe().dimensions('"Patient: " + patient_name', 'department').measures('count([patient]) / {{count(patient)}}')