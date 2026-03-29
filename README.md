**Airflow + dbt**
====================

This repository emulates an “open-source” project, though exclusively shared within the dataexpert community. Members can access the repository for independent use or contribute enhancements to the project's design and functionality. This serves as an opportunity to practice contributing to publicly shared open-source repositories.

**Table of Contents**

- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Snowflake MFA and key-pair auth](#snowflake-mfa-and-key-pair-auth)
  - [Local Development](#local-development)
    - [Debugging](#debugging)
  - [dbt Project Setup](#dbt-project-setup)
  - [Running dbt](#running-dbt)


# **🚀 Getting Started**
> ⚠️ Note: If you want to use dbt locally only, and you don't need airflow (like in the first dbt week), you can skip to [dbt Project Setup](#dbt-project-setup).


## **Prerequisites**

1. **Install [Docker](https://docs.docker.com/engine/install/)**: Docker is a platform for packaging, distributing, and managing applications in containers.
2. **Install the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)**: Astro CLI is a command-line tool designed for working with Apache Airflow projects, streamlining project creation, deployment, and management for smoother development and deployment workflows.

## **Snowflake MFA and key-pair auth**

Please enable MFA for Snowflake (passkey recommended).

### 🔐 Steps to generate your keys

Run the commands below in order:

1. **Generate encrypted PKCS#8 private key (`.p8` file)**  
   Make sure you specify a passphrase (do not leave it empty).
   ```bash
   openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 des3 -inform PEM -out rsa_key.p8
   ```
   (Output is a `.p8` file, e.g. `rsa_key.p8`.)

2. **Generate public key file**  
   ```bash
   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
   ```

3. **Generate Snowflake-compatible public key string**  
   ```bash
   openssl rsa -in rsa_key.p8 -pubout -outform DER | base64 | tr -d '\n'
   ```

Once generated, go to the **Credentials** tab in Platform (under the program you registered) to add your RSA public key to your account.


## **Local Development**

1. **Clone the Repository**: Open a terminal, navigate to your desired directory, and clone the repository using:
    ```bash
    git clone git@github.com:DataExpert-io/airflow-dbt-project.git # clone the repo
    cd airflow-dbt-project # navigate into the new folder
    ```

    1. If you don’t have SSH configured with the GitHub CLI, please follow the instructions for [generating a new SSH key](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent) and [adding a new SSH key to your GitHub account](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account?tool=cli) in the GitHub docs.
2. **Docker Setup and Management**: Launch Docker Daemon or open the Docker Desktop app
3. **Run the Astro Project**:
    - Start Airflow on your local machine by running **`astro dev start`**
        - This will spin up 4 Docker containers on your machine, each for a different Airflow component:
            - **Postgres**: Airflow's Metadata Database, storing internal state and configurations.
            - **Webserver**: Renders the Airflow UI.
            - **Scheduler**: Monitors, triggers, and orchestrates task execution for proper sequencing and resource allocation.
            - **Triggerer**: Triggers deferred tasks.
        - Verify container creation with **`docker ps`**
    - **Access the Airflow UI**: Go to http://localhost:8080/ 
        >
        > ℹ️ Note: Running astro dev start exposes the Airflow Webserver at port **`8080`** and Postgres at port **`5431`**.
        >
        > If these ports are in use, halt existing Docker containers or modify port configurations in **`.astro/config.yaml`**.
        >
4. **Stop** the Astro Docker container by running `**astro dev stop**`
    >
    > ❗🚫❗  Remember to stop the Astro project after working to prevent issues with Astro and Docker ❗🚫❗
    >


**⭐️ TL;DR - Astro CLI Cheatsheet ⭐️**

```bash
astro dev start # Start airflow
astro dev stop # Stop airflow
astro dev restart # Restart the running Docker container
astro dev kill # Remove all astro docker components
```

### **Debugging**

If the Airflow UI isn't updating, the project seems slow, Docker behaves unexpectedly, or other issues arise, first remove Astro containers and rebuild the project:

- Run these commands:
    ```bash
    # Stop all locally running Airflow containers
    astro dev stop

    # Kill all locally running Airflow containers
    astro dev kill

    # Remove Docker container, image, and volumes
    docker ps -a | grep dataexpert-airflow-dbt | awk '{print $1}' | xargs -I {} docker rm {}
    docker images | grep ^dataexpert-airflow-dbt | awk '{print $1}' | xargs -I {} docker rmi {}
    docker volume ls | grep dataexpert-airflow-dbt | awk '{print $2}' | xargs -I {} docker volume rm {}

    # In extreme cases, clear everything in Docker
    docker system prune
    ```

- Restart Docker Desktop.
- (Re)build the container image without cache.
    ```bash
    astro dev start --no-cache
    ```


Perfect — you’re right: now that we merged everything (dbt.env variables are explained inside Step 5), **Step 6 ("About the dbt.env file")** is redundant.
We can safely remove it to keep the README shorter and cleaner.

Here’s the **final version** you can copy-paste:

---

## dbt Project Setup

> ⚠️ **Important:
Please make sure you are in a personal branch, and not main.**

### Step 1: Go to the Project Directory
```bash
cd dbt_project
```

### Step 2: Create a Virtual Environment
```bash
python3 -m venv venv # MacOS/Linux
# or
python -m venv venv # Windows/PC
```

### Step 3: Activate the Virtual Environment
```bash
source venv/bin/activate # MacOS/Linux
# or for Windows:
# CMD:
venv\Scripts\activate.bat
# PowerShell:
venv\Scripts\Activate.ps1
```

### Step 4: Install the Required Packages
```bash
pip3 install -r dbt-requirements.txt # MacOS/Linux
# or
pip install -r dbt-requirements.txt # Windows/PC
```

### Step 5: Set Environment Variables

We use **Snowflake key-pair (RSA .p8) authentication** for dbt and the Snowpark scripts. Configure the environment variables below. (Password auth is still available in `profiles.yml` but commented out.)

| Variable | Purpose |
|:---|:---|
| `STUDENT_SCHEMA` | Tells dbt which database schema to use for your personal work. Each student should have a different schema to avoid conflicts. |
| `SNOWFLAKE_USER` | Your Snowflake user. |
| `PRIVATE_KEY_PATH` | **Key-pair auth:** Path to your `rsa_key.p8` file. |
| `PRIVATE_KEY_PASSPHRASE` | **Key-pair auth:** Passphrase for encrypted `.p8` key. Leave empty or unset if your key is not encrypted. |
| `SNOWFLAKE_PASSWORD` | (Optional) Used only if you switch `profiles.yml` back to password auth. |
| `DBT_PROFILES_DIR` | Tells dbt where to find your `profiles.yml` file (set to the current folder `.`). |
| `DBT_PROJECT_DIR` | Tells dbt where to find your `dbt_project.yml` file (set to the current folder `.`). |
| `DBT_PARTIAL_PARSE` | Disables partial parsing to avoid known bugs with snapshots and sources. Setting this to `'False'` forces dbt to do a full parse every time, which is safer for our setup. |

> ⚠️ **Important:**
> Please make sure you also update the **`.env`** file in the **root directory** of the project with your `STUDENT_SCHEMA`, `SNOWFLAKE_USER`, `PRIVATE_KEY_PATH`, and (if needed) `PRIVATE_KEY_PASSPHRASE`.
> This file is used by Astronomer Airflow (Docker) when you run it from the repo root.

> ⚠️ **Note on Partial Parsing:**
> There's a known issue in `dbt-core` when using snapshot definitions (in the new YAML format) that snap a source. If you modify the source, partial parsing may cause errors—especially in environments like dbt Cloud IDE, which uses partial parsing automatically.
> To avoid this, we explicitly disable partial parsing by setting `DBT_PARTIAL_PARSE='False'`. This ensures that dbt performs a **full parse** on every run, which avoids errors.
> Since our project is small, this will not cause any noticeable performance issues.

> ⚠️ **Warning:**
> Never push your personal changes (such as your `.env` updates) to the main or production branch.
> This can cause conflicts with other students' work and break shared environments. Always keep your local changes private or work on a separate branch if needed.

---

#### MacOS/Linux

- **Temporary (for current terminal session only)**:
  ```bash
  export STUDENT_SCHEMA='your_schema' # e.g., export STUDENT_SCHEMA='john'
  export SNOWFLAKE_USER='your_snowflake_user'
  export PRIVATE_KEY_PATH='/path/to/your/rsa_key.p8'
  export PRIVATE_KEY_PASSPHRASE=''   # leave empty if key is not encrypted
  export DBT_PROFILES_DIR='.'
  export DBT_PROJECT_DIR='.'
  export DBT_PARTIAL_PARSE='False'
  ```

- **Permanent (applies to all terminal sessions)**:
  - Add the same lines to your shell configuration file (like `~/.bashrc`, `~/.zshrc`, or `~/.profile`):
    ```bash
    export STUDENT_SCHEMA='your_schema'
    export SNOWFLAKE_USER='your_snowflake_user'
    export PRIVATE_KEY_PATH='/path/to/your/rsa_key.p8'
    export PRIVATE_KEY_PASSPHRASE=''
    export DBT_PROFILES_DIR='.'
    export DBT_PROJECT_DIR='.'
    export DBT_PARTIAL_PARSE='False'
    ```
  - Then reload your shell configuration:
    ```bash
    source ~/.bashrc  # or ~/.zshrc, depending on your system
    ```

---

#### Windows/PC

- **Temporary (for current terminal session only)**:
  - **CMD**:
    ```cmd
    set STUDENT_SCHEMA=your_schema
    set SNOWFLAKE_USER=your_snowflake_user
    set PRIVATE_KEY_PATH=C:\path\to\your\rsa_key.p8
    set PRIVATE_KEY_PASSPHRASE=
    set DBT_PROFILES_DIR=.
    set DBT_PROJECT_DIR=.
    set DBT_PARTIAL_PARSE=False
    ```
  - **PowerShell**:
    ```powershell
    $env:STUDENT_SCHEMA = "your_schema"
    $env:SNOWFLAKE_USER = "your_snowflake_user"
    $env:PRIVATE_KEY_PATH = "C:\path\to\your\rsa_key.p8"
    $env:PRIVATE_KEY_PASSPHRASE = ""
    $env:DBT_PROFILES_DIR = "."
    $env:DBT_PROJECT_DIR = "."
    $env:DBT_PARTIAL_PARSE = "False"
    ```

- **Permanent**:
  - Open **Environment Variables** settings.
  - Under **User variables**, click "**New**" and create each one:
    - `STUDENT_SCHEMA` → your schema (e.g., `john`)
    - `SNOWFLAKE_USER` → your Snowflake user
    - `PRIVATE_KEY_PATH` → path to your `rsa_key.p8` file
    - `PRIVATE_KEY_PASSPHRASE` → passphrase for encrypted `.p8` key, or leave blank if not encrypted
    - `DBT_PROFILES_DIR` → `.`
    - `DBT_PROJECT_DIR` → `.`
    - `DBT_PARTIAL_PARSE` → `False`

> ⚠️ Note: Variables set with `set` or `$env:` are temporary for that terminal session only unless you add them permanently in system settings.


---

### Step 6: Test Your Connection

Run:

```bash
dbt debug
```

If everything is configured correctly, you should see output like:

 ```
    13:43:43  Running with dbt=1.9.0-b3
    13:43:43  dbt version: 1.9.0-b3
    13:43:43  python version: 3.9.6
    13:43:43  python path: .../dbt-basics/venv/bin/python3
    13:43:43  os info: macOS-15.1-arm64-arm-64bit
    13:43:44  Using profiles dir at .
    13:43:44  Using profiles.yml file at ./profiles.yml
    13:43:44  Using dbt_project.yml file at ./dbt_project.yml
    13:43:44  adapter type: snowflake
    13:43:44  adapter version: 1.8.4
    13:43:44  Configuration:
    13:43:44    profiles.yml file [OK found and valid]
    13:43:44    dbt_project.yml file [OK found and valid]
    13:43:44  Required dependencies:
    13:43:44   - git [OK found]

    13:43:44  Connection:
    13:43:44    account: aab46027.us-west-2
    13:43:44    user: dataexpert_student
    13:43:44    database: DATAEXPERT_STUDENT
    13:43:44    warehouse: COMPUTE_WH
    13:43:44    role: ALL_USERS_ROLE
    13:43:44    schema: john
    13:43:44    authenticator: None
    13:43:44    oauth_client_id: None
    13:43:44    query_tag: john
    13:43:44    client_session_keep_alive: False
    13:43:44    host: None
    13:43:44    port: None
    13:43:44    proxy_host: None
    13:43:44    proxy_port: None
    13:43:44    protocol: None
    13:43:44    connect_retries: 0
    13:43:44    connect_timeout: 10
    13:43:44    retry_on_database_errors: False
    13:43:44    retry_all: False
    13:43:44    insecure_mode: False
    13:43:44    reuse_connections: True
    13:43:44  Registered adapter: snowflake=1.8.4
    13:43:50    Connection test: [OK connection ok]

    13:43:50  All checks passed!
 ```

---

### ✅ You are now ready to start working with dbt!

---

### Quick Notes:

- `STUDENT_SCHEMA` → your personal schema (different for each user)
- `PRIVATE_KEY_PATH` → path to your `rsa_key.p8` file (key-pair auth)
- `PRIVATE_KEY_PASSPHRASE` → passphrase for encrypted `.p8` key, or leave empty
- `DBT_PROFILES_DIR` → points dbt to your `profiles.yml`
- `DBT_PROJECT_DIR` → points dbt to your `dbt_project.yml`

---

### Step 7 (Optional): Set up dbt Power User Extension in VSCode

If you use **VSCode**, you can install the [dbt Power User extension](https://marketplace.visualstudio.com/items?itemName=innoverio.vscode-dbt-power-user) to enhance your development experience with features like model navigation, documentation previews, and dbt command integration.

#### Installation:
1. Open VSCode.
2. Go to the Extensions panel (`Ctrl+Shift+X`).
3. Search for **"dbt Power User"** and click **Install**.

#### Configuration:
Follow the extension setup instructions.
If you use this extension, you must also create a `.env` file inside the `dbt_project/` folder with the following line:

```env
STUDENT_SCHEMA=<your_schema>  # e.g., STUDENT_SCHEMA=john
```

> This allows the extension to parse your `dbt_project.yml` and macros correctly using your schema.

To use this extension properly, you need to open VSCode inside the dbt_project/ folder.

---

## Running dbt

Once your environment is ready, here are some essential commands to start working with dbt:

### ✅ Install dbt Packages

Before running any models, install the packages defined in `packages.yml`:

```bash
dbt deps
```

> If you skip this step, dbt will throw errors when trying to run or compile your project.

---

### 🏗️ Build Your Models

To create all tables and views in your Snowflake schema:

```bash
dbt build
```

This runs models, tests, seeds, and snapshots (if defined). After running this, you can verify the created objects in your schema on Snowflake.

---

### 🧪 Run a Specific Model

To build only a single model:

```bash
dbt build -s your_model_name
```

You can also use other selectors like:

- `+your_model_name` → builds the model and its **parents (upstream models)**
- `your_model_name+` → builds the model and all **childs (downstream models)**
- `+your_model_name+` → builds **everything related** (parents and children)

More about selection syntax: https://docs.getdbt.com/reference/node-selection/syntax

---

### 📊 Generate and View Docs

To see a visual representation and documentation of your project:

```bash
dbt docs generate
dbt docs serve
```

This will open a web page with your dbt models, dependencies, and documentation.

---

### 🔍 Check What dbt is Running

You can inspect the compiled SQL and files generated by dbt:

- Compiled SQL: `target/compiled/`
- Executed SQL: `target/run/`

These folders show exactly what dbt sends to Snowflake, which is helpful for debugging and learning.

---

### 🧠 Pro Tips: Common dbt Commands

| Command | Purpose |
|--------|---------|
| `dbt run` | Runs only models (not tests or seeds) |
| `dbt seed` | Loads seed CSV files into your database |
| `dbt test` | Runs tests defined in `.yml` files |
| `dbt build` | Runs models + tests + seeds + snapshots |
| `dbt clean` | Removes `dbt_modules` and `target/` |
| `dbt list` | Lists models, seeds, snapshots, etc. |
| `dbt run-operation` | Executes a macro manually |
| `dbt compile` | Compiles your models without running them |
| `dbt ls -s tag:your_tag` | Selects models by tag |

> 🧩 You can combine selectors and flags for powerful workflows. For example:
> ```bash
> dbt build -s staging+ --exclude tag:skip_ci
> ```

---

# Other helpful resources for learning!

### :whale: Working with Docker

> :bulb: Understanding Docker Images and Containers:
>
>
> Docker provides lightweight and isolated environments for consistent application execution across different systems. Containers encapsulate an application's code, libraries, and dependencies into a portable unit, ensuring reproducible environments. Docker images capture snapshots of filesystems, while containers represent running instances of those images.
>
> In simple terms, you can think of Docker as a tool that creates special "boxes" for software. These boxes include everything the software needs to run, like its instructions and tools. Docker also takes pictures of these boxes, called images, to use later. When you want to use the software, you open one of these pictures, and that creates a real working "box" called a container.
>
> To learn more, explore Docker's official [Getting Started](https://docs.docker.com/get-started/) guide. For a comprehensive understanding, watch this informative [YouTube video](https://www.youtube.com/watch?v=pg19Z8LL06w) by TechWorld with Nana.
>

Here are some helpful commands to remember as you get used to working with Docker:

- To check if you have any running Docker containers, use:
    ```bash
    docker ps      # List all available containers
    docker container ls   # Equivalent to above
    docker ps -a     # List running containers
    docker container ls -a   # Equivalent to above
    ```

- To list all Docker images locally:
    ```bash
    docker images
    ```

- Use the command below to remove an image. This is useful to free up space when you have unused images. Replace `<IMAGE ID>` with the actual image ID, which you can find by running **`docker images`**.
    ```bash
    docker rmi <IMAGE ID>
    ```

- Use the **`docker prune`** command to remove/reset Docker resources. This is especially handy to clean up resources and reclaim disk space.
    ```bash
    docker images prune
    docker container prune
    docker volume prune
    docker system prune
    ```

- To learn more about Docker, check out these resources below:
    - [Docker Overview](https://docs.docker.com/get-started/)
    - Enhance your Docker knowledge with this enlightening [YouTube Tutorial](https://www.youtube.com/watch?v=pg19Z8LL06w) by TechWorld with Nana

### 📂 Navigating the Repository

> :bulb: Learn more about the features of an Astro project here!
>

Each Astro project contains various directories and files. Learn more about the structure of our Astro project below:

- **`dags`**: This directory houses Directed Acyclic Graphs (DAGs), which represent the workflows in Apache Airflow. Note: it's highly encouraged that you create DAGs in subfolders so that you can make use of the `.airflowignore` file when testing locally. Learn more below:
    - **`community/`**: Stores default example DAGs for training and local testing.
    - **`.airflowignore`**: Use this file to exclude folders from the Airflow scheduler, handy for local testing and avoiding production changes.
- **`dbt_project`**: Here lies the dbt project, accessible both locally for testing and development, and within Airflow to be used in our DAGs.
- **`Dockerfile`**: This file is based on the Astro Docker image and can be customized to include project-specific commands and/or overrides for runtime behavior. Understanding this file is optional but you're welcome to explore if you wish to dive deeper into Astro.
- **`include`** contains additional project files:
- **`requirements.txt`**: Install Python packages needed for your project by adding them to this file.
- **`airflow_settings.yaml`**: Use this local-only file to define Airflow Connections, Variables, and Pools. This allows you to manage these configurations locally instead of via the Airflow UI during DAG development.
