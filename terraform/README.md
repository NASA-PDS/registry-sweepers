# Deploying Registry-Sweepers ECS Tasks

## Setting Up SSH Authentication for a Private Terraform Module Repository
Since **pds-tf-module** is a private repo, pulling the ECS module code will require SSH Access. To access a private Terraform module repository, you need to generate an SSH key and add it to your GitHub/GitLab account.

**Note:** If you already have an existing SSH key for GitHub access, you can simply.

### Generate an SSH Key
[Link to steps on GitHub for SSH Key Generation](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)

1. Open a terminal and run:
   ```sh
   ssh-keygen -t ed25519 -C "your_email@example.com"
   ```
2. When prompted, save the key in the default location (`~/.ssh/id_rsa`) or specify a custom location (you can name your file anything you want)
3. Once your key is generated, create a config file at `~.ssh/config` with the following content. This will ensure the correct config gets used everytime.
    ```
    Host github.com
        IdentityFile ~/.ssh/custom-key-name
        User git
        IdentitiesOnly yes
    ```
4. Go to GitHub and navigate to **Settings > SSH and GPG Keys**.
5. Click **New SSH Key**, paste the copied public key, and save it.
6. Test SSH connection to confirm successful setup:
   ```sh
   ssh -T git@github.com
   ```
   If successful, you will see a message like:
   ```
   Hi <your-username>! You've successfully authenticated, but GitHub does not provide shell access.

## Using the ECS Terraform Module

### 1. Reference the Module in Your Terraform Code `ecs.tf` file
To use a module stored in a private repository, add the following to your Terraform configuration:
```hcl
module "ecs" {
  source         = "git@github.com:NASA-PDS/pds-tf-modules.git//terraform/modules/ecs"
  config_file    = var.config_file
  create_service = var.create_service
  required_tags = {
    project = var.project
    cicd    = var.cicd
  }
}
```
Ensure you have SSH access to the repository, or Terraform will fail to fetch the module.

### 2. Ensure you have the following variables defined in your `variables.tf`
```hcl
variable "aws_region" {
  description = "AWS Region"
  type        = string
  default     = "us-west-2"
}

variable "config_file" {
  description = "Path to the environment-specific YAML file (eg: dev.yaml, prod.yaml)"
  type        = string
}

variable "create_service" {
  description = "Flag to control where the ECS service should be created or not"
  type        = bool
  default     = true
}

variable "required_tags" {
  description = "Required resource tags"
  type        = map(string)
}
```

### 3. Ensure your `backend.tf` is setup correctly
Ensure the `backend.tf` file is setup correctly.
**Note:** Be sure to update the **venue** to either `dev`, `test` or `prod` depending on where you're deploying.

```hcl
terraform {
  backend "s3" {
    bucket = "pds-<venue>-infra"
    key    = "<venue>/registry_sweeper_ecs.tfstate"
    region = "us-west-2"
  }
}
```

### 4. Creating Local `.yml` and `.tfvars` Files
To store environment-specific variables locally:

#### Create a local `.tfvars` File
**Note:** The code for this file is provided on the internal registry-sweepers deployment wiki, since it cannot be uploaded to GitHub.

**Note:**
1. Since ECS service creation is optional, the `create_service` variable is a bool. It can be toggled between `true` or `false`. If it's `fasle` the ECS service will not be created.
2. Ensure the path to the `<env_config-filename>.yml` is accurate.

#### Create a `.yml` Configuration File
Modify `terraform\service\ecs\task-definitions\<env_config-filename>.yml` with your settings and exact enviornment values. This is why it cannot be uploaded to GitHub.
**Note:** The code for this file is provided on the internal registry-sweepers deployment wiki, since it cannot be uploaded to GitHub.

## Export your AWS Profile locally
Before running below Terraform commands, ensure you're logged into the correct AWS environment locally.

## Running Terraform Commands
After configuring your module, initialize and run Terraform:

1. **Initialize Terraform:**
   ```sh
   cd terraform
   terraform init
   ```

2. **Validate the Configuration:**
   ```sh
   terraform validate
   ```

3. **Plan the Changes:**
   ```sh
   terraform plan -var-file=terraform.tfvars -lock=false
   ```

4. **Apply the Changes:**
   ```sh
   terraform apply -var-file=terraform.tfvars -lock=false
   ```
