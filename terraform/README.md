# Terraform scripts for Registry-Sweepers

## Requirements

Install Terraform:

    brew install terraform


## Interfaces of the registry Sweepers

To run, the registry sweepers needs to be interfaced with external components which are not deployed with the terraform scripts here:

- a **docker image** on ECR
- an **Opensearch** service containing indices registry, registry-refs, possibly prefixed per discipline, e.g. atm-registry, atm-registry-refs, geo-registry,...


These interfaces are going to be used a arguments of the terraform scripts.


## Deploy

Initialize the backend configuration. It is used to centralize the terraform state so that it can be re-uzed by different users working on the deployment.

    cp backend-config.tfvars.example backend-config.tfvars

Update the region, bucket and key to be used.

Initialize the parameters, starting from the terraform.tfvars.example file provided.

Copy it:

    cp terraform.tfvars.example terraform.tfvars

And update the values.


Run the terraform scripts to create the task definition to run the sweepers for each node:




```
    terraform init -backend-config=backend-config.tfvars
    terraform plan
    terraform apply
```

# Generate the DAG files

The sweepers are runs as DAG (worfklows) in Airflow. The following step generate the python DAG file for each node:

First set your venue environment:

    cp generate_dags.env.example generate_dags.env

Update the copied file with your environment.

Generate the DAG file:

    python generate_dags.py
