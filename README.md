# GuardianVision

**GuardianVision** is an AI-powered safety monitoring app that analyzes live video feeds from cameras every 30 seconds. It detects safety violations based on customizable checklists for construction and maintenance sites, using a multi-modal LLM to generate real-time alerts and help prevent accidents.

## Features

- **Real-time Monitoring:** Analyzes video feeds every 30 seconds.
- **Customizable Checklists:** Tailor the safety criteria based on the specific site and activity (e.g., construction, maintenance).
- **AI-powered Violation Detection:** Uses a multi-modal LLM to identify safety violations and issue alerts.
- **Prevention-focused:** Designed to reduce accidents by ensuring compliance with safety regulations.

## Installation

To get started with GuardianVision, follow these steps:

1. Clone the repository:
    ```bash
    git clone https://github.com/yourusername/guardianvision.git
    ```

2. Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Set up your camera feeds and configure the app using the provided templates.

## Usage

After installation, you can launch the app with the following command:
```bash
python guardianvision.py


The app will start monitoring the video feeds and will generate alerts for any detected safety violations based on the checklists you've set up.

Citation
If you use this project or datasets in your work, please cite the following:

Construction Site Dataset

@misc{
    construction-site_dataset,
    title = { Construction Site  Dataset },
    type = { Open Source Dataset },
    author = { New Workspace },
    howpublished = { \url{ https://universe.roboflow.com/new-workspace-w7psa/construction-site } },
    url = { https://universe.roboflow.com/new-workspace-w7psa/construction-site },
    journal = { Roboflow Universe },
    publisher = { Roboflow },
    year = { 2022 },
    month = { dec },
    note = { visited on 2024-10-24 },
}

@misc{
    construction-site-safety_dataset,
    title = { Construction Site Safety Dataset },
    type = { Open Source Dataset },
    author = { Roboflow Universe Projects },
    howpublished = { \url{ https://universe.roboflow.com/roboflow-universe-projects/construction-site-safety } },
    url = { https://universe.roboflow.com/roboflow-universe-projects/construction-site-safety },
    journal = { Roboflow Universe },
    publisher = { Roboflow },
    year = { 2024 },
    month = { aug },
    note = { visited on 2024-10-28 },
    }
Other Sources
(Add citation information for other sources used in the project, following a similar format as above.)

License and Attribution
This project uses the following datasets and resources:

Construction Site Dataset
[Other Source Name and Link]
Make sure to comply with the licenses and terms of use for each dataset or resource.

License
This project is licensed under the MIT License - see the LICENSE file for details.
