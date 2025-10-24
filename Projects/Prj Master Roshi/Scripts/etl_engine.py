import pandas as pd
import os
from datetime import datetime

# --- CONFIGURATION FOR RELATIVE PATHS ---

# GET THE ABSOLUTE PATH OF THE DIRECTORY CONTAINING THE CURRENTLY EXECUTING SCRIPT

script_dir = os.path.dirname(os.path.abspath(__file__))

# Navigate up one level to the Project Root Directory
project_root = os.path.dirname(script_dir)

# Define the paths to the Data directory and the Archive directory

data_dir = os.path.join(project_root, "Data")
archive_dir = os.path.join(project_root, "Archive")
master_db_path = os.path.join(data_dir, "Master_Database.xlsx")
update_queue_path = os.path.join(data_dir, "update_queue.xlsx")

print(f"Project Base Directory set to :{project_root}")
print(f"Data Directory set to :{data_dir}")
print(f"Archive Directory set to :{archive_dir}")
print(f"Master Database Path set to :{master_db_path}")


def initialize_master_database():  # <--- Master Data Base initialization
    """Creates the master_database.xlsx with all three required sheets and initial data."""

    print("Master Database not found. Creating a template file with initial data...")
    sheets = []

    # 1. Employees Sheet Data
    employees_data = {
        "ACF2_ID": [
            "TEST001",
            "TEST002",
            "TEST003",
        ],
        "First_Name": ["John", "Jane", "Bob"],
        "Last_Name": ["Doe", "Smith", "Johnson"],
        "Team_ID": ["STMT", "PAYT", "PAYT"],
        "Status": ["Active", "Active", "Active"],
    }
    employees_df = pd.DataFrame(
        employees_data
    )  # All columns are of object type (strings)
    sheets.append(("Employees", employees_df))

    # 2. Skills Sheet Data

    skills_data = {
        "Skill_ID": ["STMT1", "STMT2", "PAYT1", "PAYT2"],
        "Skill_Name": [
            "Process_Statements",
            "Penalty_Waiver_Review",
            "Process_Payout_Exception",
            "Approve_Payout_Exception",
        ],
        "Team_ID": ["STMT", "STMT", "PAYT", "PAYT"],
    }

    skills_df = pd.DataFrame(skills_data)  # All columns are of object type
    sheets.append(("Skills", skills_df))

    # 3. Teams Sheet Data

    teams_data = {
        "Team_ID": ["STMT", "PAYT", "MAINT", "DPER"],
        "Team_Name": ["Statement", "Payout", "Maintenance", "Doc Prep & E-Reg"],
        "Manager": [
            "Kamal Douglas",
            "Rahul Verma",
            "Mary Mole",
            "Vanessa E Malendez",
        ],
    }
    teams_df = pd.DataFrame(teams_data)  # All columns are of object type
    sheets.append(("Teams", teams_df))

    # 4. Employee_Skills_Map sheet Data

    map_data = {
        "ACF2_ID": ["TEST001", "TEST001", "TEST002"],
        "Skill_ID": ["STMT1", "PAYT2", "DPER1"],
        "Proficiency_Level": [1, 2, 3],
        "Certification_Date": [
            datetime(2025, 10, 3),
            datetime(2024, 5, 15),
            datetime(2023, 1, 1),
        ],
    }

    map_df = pd.DataFrame(map_data)
    sheets.append(("Employee_Skills_Map", map_df))

    # Write all dataframes to the single excel file.
    try:
        # use openpyxl engine to handle the modern excel format
        with pd.ExcelWriter(master_db_path, engine="openpyxl") as writer:
            for name, df in sheets:
                df.to_excel(writer, sheet_name=name, index=False)
                print(f"SUCCESS: Master Database created at: {master_db_path}")

    except Exception as e:
        print(f"ERROR: Failed to write the database file: {e}")


def load_master_data():
    """Loads all 4 sheets from the Master Database for processing."""
    try:
        master_dfs = pd.read_excel(
            master_db_path,
            sheet_name=["Employees", "Skills", "Teams", "Employee_Skills_Map"],
        )
        return master_dfs
    except FileNotFoundError:
        print("ERROR: Master Database file not found during load.")
        return None
    except Exception as e:
        print(f"ERROR: An error occurred while loading data: {e}")
        return None


# --- PHASE 2 CORE ETL LOGIC ---


def process_all_updates():
    """Reads all update sheets, processes romals first, then additions, and updates the master database."""
    print("\n--- Processing all updates ---")

    # 1. Load master and Update Data
    try:
        master = load_master_data()
        # Read all sheets in a Update Queue, even if some are empty
        update_sheets = pd.read_excel(update_queue_path, sheet_name=None)
    except FileNotFoundError:
        print(
            f"ERROR: Update Queue file not found at {update_queue_path}. Please ensure it exists."
        )
        return
    except Exception as e:
        print(f"ERROR during initial data load: {e}")
        return

    # Initialize list to hold rejected records
    rejected_records = []

    # --- CRITICAL FIX: Initialize removal ID lists ---
    remove_ids = []  # For Employee ACF2_IDs
    remove_skills = []  # For Skill Skill_IDs

    # --------------------------------------------------------------------
    # Step 2: PROCESS REMOVALS (Prioritized for data hygiene)
    # --------------------------------------------------------------------

    print("\n[STEP 2/4] Processing REMOVALS...")

    # a. Remove Employees (Requires cascade removal from map)
    if (
        master is not None
        and "Remove_Employee" in update_sheets
        and not update_sheets["Remove_Employee"].empty
    ):
        df_rem_emp = update_sheets["Remove_Employee"].dropna(subset=["ACF2_ID"])
        remove_ids = df_rem_emp["ACF2_ID"].astype(str).unique()
        if "Employees" in master:
            master["Employees"] = master["Employees"][
                ~master["Employees"]["ACF2_ID"].isin(remove_ids)
            ]
        else:
            print(
                "ERROR: 'Employees' sheet not found in master database. Skipping employee removal."
            )
    elif master is None:
        print("ERROR: Master data not loaded, cannot process removals.")
    # Cascade: Remove training records from removed employees
    master["Employee_Skills_Map"] = master["Employee_Skills_Map"][
        ~master["Employee_Skills_Map"]["ACF2_ID"].isin(remove_ids)
    ]
    print(
        f"  - Removed {len(remove_ids)} employee(s) and their associated training records."
    )

    # b. Remove Skills (Requires cascade removal from map)

    if (
        master is not None
        and "Remove_Skill" in update_sheets
        and not update_sheets["Remove_Skill"].empty
    ):
        df_rem_skill = update_sheets["Remove_Skill"].dropna(subset=["Skill_ID"])
        remove_skills = df_rem_skill["Skill_ID"].astype(str).unique()
        if "Skills" in master:
            master["Skills"] = master["Skills"][
                ~master["Skills"]["Skill_ID"].isin(remove_skills)
            ]
        else:
            print(
                "ERROR: 'Skills' sheet not found in master database. Skipping skill removal."
            )
    elif master is None:
        print("ERROR: Master data not loaded, cannot process removals.")

    # c. Remove Teams
    if "Remove_Team" in update_sheets and not update_sheets["Remove_Team"].empty:
        df_rem_team = update_sheets["Remove_Team"].dropna(subset=["Team_ID"])
        remove_team_ids = df_rem_team["Team_ID"].astype(str).unique()

        # Validation: Check if any active employees belong to these teams
        active_employees_on_team = master["Employees"][
            master["Employees"]["Team_ID"].isin(remove_team_ids)
        ]

        if not active_employees_on_team.empty:
            print(
                f"  - WARNING: Cannot remove {len(remove_team_ids)} team(s) as {len(active_employees_on_team)} active employees still linked."
            )
            # For simplicity, we just won't remove them. In production , you'd reject the transaction
        else:
            master["Teams"] = master["Teams"][
                ~master["Teams"]["Team_ID"].isin(remove_team_ids)
            ]
            print(f"   - Removed {len(remove_team_ids)} teams(s).")

    # Cascade: Remove training records for removed skills
    master["Employee_Skills_Map"] = master["Employee_Skills_Map"][
        ~master["Employee_Skills_Map"]["Skill_ID"].isin(remove_skills)
    ]
    print(
        f"  - Removed {len(remove_skills)} skill(s) and their associated training records."
    )

    # --------------------------------------------------------------------
    # Step 3: PROCESS ADDITIONS & UPDATES (Including validation)
    # --------------------------------------------------------------------

    print("\n[STEP 3/4] Processing ADDITIONS & UPDATES...")

    # a. Add Teams
    if "Add_Team" in update_sheets and not update_sheets["Add_Team"].empty:
        new_teams = update_sheets["Add_Team"].dropna(
            subset=[
                "Team_ID",
                "Team_Name",
            ]
        )
        new_teams["Team_ID"] = new_teams["Team_ID"].astype(str)
        existing_ids = set(master["Teams"]["Team_ID"].astype(str).values)

        valid_adds = new_teams[~new_teams["Team_ID"].isin(existing_ids)]
        rejected_records.extend(
            new_teams[new_teams["Team_ID"].isin(existing_ids)]
            .assign(Reason="Duplicate Team_ID")
            .to_dict("records")
        )

        master["Teams"] = pd.concat([master["Teams"], valid_adds], ignore_index=True)
        print(
            f"  - Added {len(valid_adds)} new team(s). Rejected {len(new_teams) - len(valid_adds)} duplicates."
        )

    # Update Teams (Manager, Team_Name)
    if "Update_Team" in update_sheets and not update_sheets["Update_Team"].empty:
        updates = update_sheets["Update_Team"].dropna(subset=["Team_ID"])
        updates["Team_ID"] = updates["Team_ID"].astype(str)

        # Identify teams that exist in the master and need updating
        existing_teams_to_update = master["Teams"][
            master["Teams"]["Team_ID"].isin(updates["Team_ID"])
        ]

        # Apply updates to the existing teams
        for index, row in updates.iterrows():
            team_id = row["Team_ID"]
            # Update 'Manager' and 'Team_Name' if they are present in the update row
            if "Manager" in row and pd.notna(row["Manager"]):
                master["Teams"].loc[
                    master["Teams"]["Team_ID"] == team_id, "Manager"
                ] = row["Manager"]
            if "Team_Name" in row and pd.notna(row["Team_Name"]):
                master["Teams"].loc[
                    master["Teams"]["Team_ID"] == team_id, "Team_Name"
                ] = row["Team_Name"]

        # Identify updates for non-existent teams
        non_existent_updates = updates[
            ~updates["Team_ID"].isin(master["Teams"]["Team_ID"])
        ]
        rejected_records.extend(
            non_existent_updates.assign(Reason="Team_ID not found for update").to_dict(
                "records"
            )
        )

        # Log the number of updates that actually occurred
        num_updated = len(existing_teams_to_update)
        num_rejected = len(updates) - num_updated
        print(
            f"  - Updated {num_updated} team(s) with new information. Rejected {num_rejected} updates for non-existent teams."
        )

    # b. Add New Employees (Validation: Employee ID unique, Team ID exists)
    if "Add_Employee" in update_sheets and not update_sheets["Add_Employee"].empty:
        new_employees = update_sheets["Add_Employee"].dropna(
            subset=["ACF2_ID", "First_Name", "Last_Name", "Team_ID"]
        )
        existing_ids = set(master["Employees"]["ACF2_ID"].astype(str).values)
        current_team_ids = set(master["Teams"]["Team_ID"].astype(str).values)

        valid_adds = []

        for index, row in new_employees.iterrows():
            if row["ACF2_ID"] in existing_ids:
                row["Reason"] = "Duplicate ACF2_ID"
                rejected_records.append(row.to_dict())
                continue
            if row["Team_ID"] not in current_team_ids:
                row["Reason"] = "Team ID does not exist in Master Teams."
                rejected_records.append(row.to_dict())
                continue
            valid_adds.append(row)

        if valid_adds:
            df_valid_adds = pd.DataFrame(valid_adds)
            master["Employees"] = pd.concat(
                [master["Employees"], df_valid_adds], ignore_index=True
            )
            print(f"  - Added {len(df_valid_adds)} new employee(s).")
        print(
            f"  - Rejected {len(new_employees) - len(valid_adds)} duplicates/invalid entries."
        )

    # c. Add New Skills
    if "Add_Skill" in update_sheets and not update_sheets["Add_Skill"].empty:
        new_skills = update_sheets["Add_Skill"].dropna(
            subset=["Skill_ID", "Skill_Name"]
        )
        new_skills["Skill_ID"] = new_skills["Skill_ID"].astype(
            str
        )  # Enforce a string type
        existing_skills = set(master["Skills"]["Skill_ID"].values)

        valid_adds = new_skills[~new_skills["Skill_ID"].isin(existing_skills)]
        rejected_records.extend(
            new_skills[new_skills["Skill_ID"].isin(existing_skills)]
            .assign(Reason="Duplicate Skill_ID")
            .to_dict("records")
        )

        master["Skills"] = pd.concat([master["Skills"], valid_adds], ignore_index=True)
        print(
            f"  - Added {len(valid_adds)} new skill(s). Rejected: {len(new_skills)-len(valid_adds)} duplicate records"
        )

    # d. Add New Training to Map (The Critical Validation step)
    if (
        "Add_Training_Map" in update_sheets
        and not update_sheets["Add_Training_Map"].empty
    ):
        updates = update_sheets["Add_Training_Map"].dropna(
            subset=["ACF2_ID", "Skill_ID", "Proficiency_Level", "Certification_Date"]
        )
        updates["Skill_ID"] = updates["Skill_ID"].astype(
            str
        )  # Enforces tha data type to be string

        valid_training_adds = []

        # Get current IDs/Skills for validation check
        current_employee_ids = set(master["Employees"]["ACF2_ID"].values)
        current_skill_ids = set(master["Skills"]["Skill_ID"].astype(str).values)

        for index, row in updates.iterrows():
            # Validation Check 1 Employee ID must exist
            if row["ACF2_ID"] not in current_employee_ids:
                row["Reason"] = "Employee ID does not exist"
                rejected_records.append(row.to_dict())
                continue
            # Validation Check 2 Skill ID must exist
            if row["Skill_ID"] not in current_skill_ids:
                row["Reason"] = "Skill ID does not exist."
                rejected_records.append(row.to_dict())
                continue

            # Remove old certification for the same employee/skill pair (Update/Overwrite)
            master["Employee_Skills_Map"] = master["Employee_Skills_Map"][
                ~(
                    (master["Employee_Skills_Map"]["ACF2_ID"] == row["ACF2_ID"])
                    & (master["Employee_Skills_Map"]["Skill_ID"] == row["Skill_ID"])
                )
            ]

            valid_training_adds.append(row)

        if valid_training_adds:
            updates_to_add = pd.DataFrame(valid_training_adds)
            master["Employee_Skills_Map"] = pd.concat(
                [master["Employee_Skills_Map"], updates_to_add], ignore_index=True
            )
            print(
                f"  - Added/Updated {len(updates_to_add)} training records to the map. Rejected {len(updates) - len(updates_to_add)} invalid entries."
            )

    # --------------------------------------------------------------------
    # Step 4: WRITE MASTER DATA, ARCHIVE, AND REPORT REJECTIONS
    # --------------------------------------------------------------------
    print("\n[STEP 4/4] Finalizing changes...")

    # Write Master DataFrames back to the single Master_Database.xlsx
    try:
        with pd.ExcelWriter(master_db_path, engine="openpyxl") as writer:
            for name, df in master.items():
                df.to_excel(writer, sheet_name=name, index=False)
        print(f"SUCCESS: Master Database updated at: {master_db_path}")
    except Exception as e:
        print(f"ERROR: Failed to write to Master Database. Check file permissions: {e}")
        return

    # Handle Rejections

    if rejected_records:
        rejected_df = pd.DataFrame(rejected_records)
        rejected_path = os.path.join(archive_dir, "rejected_records.xlsx")
        rejected_df.to_excel(rejected_path, index=False)
        print(f"Rejected records written to {rejected_path}")
        print(
            f"WARNING: {len(rejected_records)} records were rejected. See Rejected file in Archive."
        )

    # Archive the processed update queue

    archive_name = f'PROCESSED_updates_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
    os.rename(update_queue_path, os.path.join(archive_dir, archive_name))
    print(f"SUCCESS: Update Queue archived. ETL process finished.")


# --- PHASE TESTING ---


def run_phase_1_tests():
    """Tests the integrity of the read/write process
    by adding and verifying a new skill."""

    print("\n--- Running Phase 1 Tests Read/Write Test---")

    master_data = load_master_data()
    if master_data is None:
        print("Test Failed: Could not load master data.")
        return

    # TEST Check initial data count
    print(f"Initial Skills loaded: {len(master_data['Skills'])} records.")

    # TEST: Simple modification (WRITE test) Add a Python skill relevant to your project
    new_skill_data = pd.DataFrame(
        {
            "Skill_ID": ["DPER1"],
            "Skill_Name": ["Doc Prep"],
            "Skill_Team": ["DPER"],
        }
    )

    # Concatenate the new skill and remove potential duplicates (keep the new one)

    updated_skills_df = pd.concat(
        [master_data["Skills"], new_skill_data], ignore_index=True
    ).drop_duplicates(subset=["Skill_ID"], keep="last")

    # Write only the updated Skills sheet back to the Master file

    try:
        with pd.ExcelWriter(
            master_db_path, engine="openpyxl", mode="a", if_sheet_exists="overlay"
        ) as writer:
            updated_skills_df.to_excel(writer, sheet_name="Skills", index=False)
        print("Skills sheet successfuly updated with a new record Doc Prep")
    except Exception as e:
        print(f"Test WRITE Failed during overwrite{e}")
        return

    # TEST: Verify modification (READ test)
    verified_data = load_master_data()
    if "DPER1" in verified_data["Skills"]["Skill_ID"].values:
        print(
            "Test Passed! 'DPER1' skill found in the reloaded data. Foundation is solid."
        )
    else:
        print("Test Failed: 'DPER1' skill not found in the reloaded data.")


def run_phase_2_tests():
    """Sets up and runs a comprehensive test using the Update_Queue.xlsx."""
    print("\n--- Running Phase 2 Comprehensive Tesd ---")

    update_data = {
        # TRANSACTION 1: Remove Team STMT (Statements Team). Should fail validation (employee TEST001 is still linked)
        "Remove_Team": pd.DataFrame({"Team_ID": ["STMT"]}),
        # TRANSACTION 2: Add a new valid team (Team CMT). Should suceed.
        "Add_Team": pd.DataFrame(
            {
                "Team_ID": ["CMT"],
                "Team_Name": "Credit Maintenance",
                "Manager": ["Eric Melo"],
            }
        ),
        # TRANSACTION 3: Remove Skill PAYT2 (Approve_Payout_Exception). Should suceed and cascade delete map entries for PAYT2.
        "Remove_Skill": pd.DataFrame({"Skill_ID": ["PAYT2"]}),
        # TRANSACTION 4: Add new employee: TEST004 (valid, Team 40 added above). TEST005 (invalid, Team 99).
        "Add_Employee": pd.DataFrame(
            {
                "ACF2_ID": ["TEST004", "TEST005"],
                "First_Name": ["Samantha", "Robert"],
                "Last_Name": ["White", "Mateuse"],
                "Team_ID": ["CMT", "99"],
                "Status": ["Active", "Active"],
            }
        ),
        # TRANSACTION 5: Add new skill 902. Should succeed.
        "Add_Skill": pd.DataFrame(
            {
                "Skill_ID": ["MAINT01"],
                "Skill_Name": ["Data Entry"],
                "Team_ID": ["MAINT"],
            }
        ),
        # TRANSACTION 6:
        # - Rec 1: Valid (E004 gets new skill 902). Succeeds.
        # - Rec 2: Invalid Employee ID (E999). Rejected by validation.
        # - Rec 3: Valid employee E0810 getting removed skill DPER1. Rejected by validation (Skill DPER1 is removed in Trans 3).
        "Add_Training_Map": pd.DataFrame(
            {
                "ACF2_ID": ["TEST002", "TEST005", "TEST004"],
                "Skill_ID": ["PAYT1", "STMT1", "DPER1"],
                "Proficiency_Level": [1, 1, 1],
                "Certification_Date": [datetime.now(), datetime.now(), datetime.now()],
            }
        ),
        # TRANSACTION 7: Remove Employee TEST003 (Bob). Should succeed and remove map entries.
        "Remove_Employee": pd.DataFrame({"ACF2_ID": ["TEST003"]}),
    }

    try:
        # Create/overwrite the Update_Queue.xlsx with all sheets for testing
        with pd.ExcelWriter(update_queue_path, engine="openpyxl") as writer:
            for sheet_name, df in update_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print("Test Update_Queue.xlsx created with 7 transaction types.")

        # Run the full ETL process
        process_all_updates()

    except Exception as e:
        print(f"ERROR: Phase 2 test failed during setup or execution: {e}")


# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    # Ensure directories exists
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    # Step 1: Initialization
    if os.path.exists(master_db_path):
        print(
            "\nMaster DB exists. Deleting and re-initializing to ensure 4-sheet structure for test."
        )
        os.remove(master_db_path)

    initialize_master_database()

    # Step 2: Run verification tests

    run_phase_2_tests()
