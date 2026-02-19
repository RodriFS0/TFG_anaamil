# Fork from Ana Amil
This repository is used to Update the database "RepoRT_classified" used to train the machine learning models in the repository TFG_RodrigoFernandez. To execute this code you will need three datasets:
1- "temporal/all_classified" (provided in the repository) wich cointains more than 5 milion clasified molecules.
2- "external/RepoRT_remote/process_data" that makes reference to the RepoRT database from @michaelwitting in his repository "RepoRT", escecificly the forder "processed_data". This Database its countinuosly growing, but the algorithm already checks and updates the database in this repository.

# USER MANUAL
There are two main ways:
1- RUN from IDE: Clone this repository in your IDE and open the RepoRT_classified_Developer.py scrypt and run it. After all the blocks are processed the file will saved in the proyect of the repository, but also there should be a "save as" window for you to chose where to save it and under the name of your preference.
2- RUN from CLI: Clone this repository and go to the terminal of your computer, then go to the path where the repository is located localy in your computer (eg: cd \Users\name\Desktop\proyects\TFG_anaamil) and run the following message: .\.venv\Scripts\python -m temporal.RepoRT_classified_Developer --classified temporal/all_classified.tsv . The code will run the same way ass before, updating the existing RepoRT_classified file in the repository and also appering the "save as" window.
