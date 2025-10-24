import pandas as pd
import os
from datetime import datetime, date

# --- CONFIGURATION FOR PORTABILITY (Relative Paths) ---
# This block automatically finds the project folders regardless of the drive/location.
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    BASE_DIR = os.path.dirname(
        SCRIPT_DIR
    )  # Assumes script is in /SkillTracker_Project/Scripts/
except NameError:
    # Fallback for environments where __file__ is not defined (e.g., interactive shells)
    print("Warning: __file__ not defined. Set BASE_DIR manually.")
    BASE_DIR = "C:/temp/SkillTracker_Project"  # Replace with actual ABSOLUTE path if necessary!

DATA_DIR = os.path.join(BASE_DIR, "Data")
ARCHIVE_DIR = os.path.join(BASE_DIR, "Archive")
MASTER_DB_PATH = os.path.join(DATA_DIR, "Master_Database.xlsx")
UPDATE_QUEUE_PATH = os.path.join(DATA_DIR, "Update_Queue.xlsx")

print(f"Project Base Directory set to: {BASE_DIR}")


# --- UTILITY FUNCTIONS ---


def initialize_master_database():
    """Creates the Master_Database.xlsx with all four required sheets and initial data."""
    print("Master Database not found. Creating a template file with initial data...")

    # 1. TEAM_DATA Sheet (New addition)
    team_data = {
        "Team_ID": [10, 20, 30],
        "Team_Name": ["Ops Floor 1", "Training Dept", "Ops Management"],
        "Manager": ["Carl Douglas", "Saya Dan", "Sandra Douglas"],
    }
    team_df = pd.DataFrame(team_data)

    # 2. EMPLOYEES Sheet Data (Now uses Team_ID)
    employees_data = {
        "Employee_ID": ["E0810", "E1225", "E003"],
        "Name": ["Kamal Douglas", "Saya Dan", "Carl Douglas"],
        "Team_ID": [10, 20, 30],  # Using numerical Team_ID
        "Status": ["Active", "Active", "Active"],
    }
    employees_df = pd.DataFrame(employees_data)

    # 3. SKILLS Sheet Data (Numerical Skill_Code)
    skills_data = {
        "Skill_Code": [101, 102, 205, 310],
        "Skill_Name": [
            "Standard Operational Procedure 101",
            "Mandatory Safety Training",
            "Data Analysis Prep",
            "Forklift Certification",
        ],
        "Category": ["SOP", "Safety", "Technical", "Equipment"],
        "Required_Ops_Area": ["All", "All", "IT/Ops Support", "Warehouse"],
    }
    skills_df = pd.DataFrame(skills_data)

    # 4. EMPLOYEE_SKILLS_MAP Sheet Data
    map_data = {
        "Employee_ID": ["E0810", "E0810", "E1225"],
        "Skill_Code": [101, 102, 101],
        "Certification_Date": [
            datetime(2025, 1, 1),
            datetime(2025, 1, 1),
            datetime(2025, 9, 1),
        ],
        "Expiration_Date": [pd.NaT, datetime(2026, 1, 1), pd.NaT],
        "Trainer": ["C. Smith", "S. Dan", "K. Douglas"],
    }
    map_df = pd.DataFrame(map_data)

    # Write all four DataFrames
    try:
        with pd.ExcelWriter(MASTER_DB_PATH, engine="openpyxl") as writer:
            employees_df.to_excel(writer, sheet_name="Employees", index=False)
            team_df.to_excel(writer, sheet_name="Team_Data", index=False)
            skills_df.to_excel(writer, sheet_name="Skills", index=False)
            map_df.to_excel(writer, sheet_name="Employee_Skills_Map", index=False)
        print(f"SUCCESS: Master Database created with 4 sheets.")
    except Exception as e:
        print(f"ERROR: Failed to write the database file: {e}")


def load_master_data():
    """Loads all four sheets from the Master Database for processing."""
    try:
        master_dfs = pd.read_excel(
            MASTER_DB_PATH,
            sheet_name=["Employees", "Team_Data", "Skills", "Employee_Skills_Map"],
        )
        return master_dfs
    except FileNotFoundError:
        print("ERROR: Master Database file not found during load.")
        return None
    except Exception as e:
        print(f"ERROR: An error occurred while loading data: {e}")
        return None


# --- CORE ETL LOGIC ---


def process_all_updates():
    """Reads all update sheets, processes removals first, then additions, and updates the master database."""
    print("\n--- Starting Comprehensive ETL Process ---")

    # 1. Load Master and Update Data
    try:
        master = load_master_data()
        # Read all sheets in the Update Queue, treating potential missing sheets gracefully
        update_sheets = pd.read_excel(UPDATE_QUEUE_PATH, sheet_name=None)
    except FileNotFoundError:
        print(
            f"ERROR: Update Queue file not found at {UPDATE_QUEUE_PATH}. Please ensure it exists."
        )
        return
    except Exception as e:
        print(f"ERROR during initial data load: {e}")
        return

    # Initialize list to hold rejected records
    rejected_records = []

    # --- Step 2: PROCESS REMOVALS (Prioritized for data hygiene) ---
    print("\n[STEP 2/4] Processing REMOVALS...")

    # a. Remove Teams
    if "Remove_Team" in update_sheets and not update_sheets["Remove_Team"].empty:
        df_rem_team = update_sheets["Remove_Team"].dropna(subset=["Team_ID"])
        remove_team_ids = df_rem_team["Team_ID"].astype(int).unique()

        # Validation: Check if any active employees belong to these teams
        active_employees_on_team = master["Employees"][
            master["Employees"]["Team_ID"].isin(remove_team_ids)
        ]

        if not active_employees_on_team.empty:
            print(
                f"  - WARNING: Cannot remove {len(remove_team_ids)} team(s) as {len(active_employees_on_team)} active employees are still linked."
            )
            # In a real-world scenario, you might reassign or reject. Here, we skip the removal.
        else:
            master["Team_Data"] = master["Team_Data"][
                ~master["Team_Data"]["Team_ID"].isin(remove_team_ids)
            ]
            print(f"  - Removed {len(remove_team_ids)} team(s).")

    # b. Remove Employees (Requires cascade removal from map)
    if (
        "Remove_Employee" in update_sheets
        and not update_sheets["Remove_Employee"].empty
    ):
        df_rem_emp = update_sheets["Remove_Employee"].dropna(subset=["Employee_ID"])
        remove_ids = df_rem_emp["Employee_ID"].astype(str).unique()

        # Remove employee from master Employees sheet
        master["Employees"] = master["Employees"][
            ~master["Employees"]["Employee_ID"].isin(remove_ids)
        ]

        # CASCADE: Remove training records for removed employees (Crucial integrity step!)
        master["Employee_Skills_Map"] = master["Employee_Skills_Map"][
            ~master["Employee_Skills_Map"]["Employee_ID"].isin(remove_ids)
        ]
        print(
            f"  - Removed {len(remove_ids)} employee(s) and their associated training records."
        )

    # c. Remove Skills (Requires cascade removal from map)
    if "Remove_Skill" in update_sheets and not update_sheets["Remove_Skill"].empty:
        df_rem_skill = update_sheets["Remove_Skill"].dropna(subset=["Skill_Code"])
        remove_codes = df_rem_skill["Skill_Code"].astype(int).unique()

        # Remove skill from master Skills sheet
        master["Skills"] = master["Skills"][
            ~master["Skills"]["Skill_Code"].isin(remove_codes)
        ]

        # CASCADE: Remove training records for removed skills
        master["Employee_Skills_Map"] = master["Employee_Skills_Map"][
            ~master["Employee_Skills_Map"]["Skill_Code"].isin(remove_codes)
        ]
        print(
            f"  - Removed {len(remove_codes)} skill(s) and their associated training records."
        )

    # --- Step 3: PROCESS ADDITIONS & UPDATES (Including validation) ---
    print("\n[STEP 3/4] Processing ADDITIONS & UPDATES...")

    # a. Add Teams
    if "Add_Team" in update_sheets and not update_sheets["Add_Team"].empty:
        new_teams = update_sheets["Add_Team"].dropna(subset=["Team_ID", "Team_Name"])
        new_teams["Team_ID"] = new_teams["Team_ID"].astype(int)
        existing_ids = set(master["Team_Data"]["Team_ID"].astype(int).values)

        valid_adds = new_teams[~new_teams["Team_ID"].isin(existing_ids)]
        rejected_records.extend(
            new_teams[new_teams["Team_ID"].isin(existing_ids)]
            .assign(Reason="Duplicate Team ID")
            .to_dict("records")
        )

        master["Team_Data"] = pd.concat(
            [master["Team_Data"], valid_adds], ignore_index=True
        )
        print(
            f"  - Added {len(valid_adds)} new team(s). Rejected {len(new_teams) - len(valid_adds)} duplicates."
        )

    # b. Add New Employees (Validation: Employee ID unique, Team ID exists)
    if "Add_Employee" in update_sheets and not update_sheets["Add_Employee"].empty:
        new_employees = update_sheets["Add_Employee"].dropna(
            subset=["Employee_ID", "Name", "Team_ID"]
        )
        new_employees["Team_ID"] = new_employees["Team_ID"].astype(int)
        existing_ids = set(master["Employees"]["Employee_ID"].values)
        current_team_ids = set(master["Team_Data"]["Team_ID"].astype(int).values)

        valid_adds = []

        for index, row in new_employees.iterrows():
            if row["Employee_ID"] in existing_ids:
                row["Reason"] = "Duplicate Employee ID."
                rejected_records.append(row.to_dict())
                continue
            # VALIDATION: Team_ID must exist
            if row["Team_ID"] not in current_team_ids:
                row["Reason"] = "Team ID does not exist in Master Team Data."
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

    # c. Add New Skills (Validation: Skill Code unique)
    if "Add_Skill" in update_sheets and not update_sheets["Add_Skill"].empty:
        new_skills = update_sheets["Add_Skill"].dropna(
            subset=["Skill_Code", "Skill_Name"]
        )
        new_skills["Skill_Code"] = new_skills["Skill_Code"].astype(int)
        existing_codes = set(master["Skills"]["Skill_Code"].astype(int).values)

        valid_adds = new_skills[~new_skills["Skill_Code"].isin(existing_codes)]
        rejected_records.extend(
            new_skills[new_skills["Skill_Code"].isin(existing_codes)]
            .assign(Reason="Duplicate Skill Code")
            .to_dict("records")
        )

        master["Skills"] = pd.concat([master["Skills"], valid_adds], ignore_index=True)
        print(
            f"  - Added {len(valid_adds)} new skill(s). Rejected {len(new_skills) - len(valid_adds)} duplicates."
        )

    # d. Add New Training to Map (Validation: Employee ID & Skill Code must exist; Overwrite)
    if (
        "Add_Training_Map" in update_sheets
        and not update_sheets["Add_Training_Map"].empty
    ):
        updates = update_sheets["Add_Training_Map"].dropna(
            subset=["Employee_ID", "Skill_Code", "Certification_Date"]
        )
        updates["Skill_Code"] = updates["Skill_Code"].astype(int)

        valid_training_adds = []
        current_employee_ids = set(master["Employees"]["Employee_ID"].values)
        current_skill_codes = set(master["Skills"]["Skill_Code"].astype(int).values)

        for index, row in updates.iterrows():
            # Validation 1: Employee ID must exist
            if row["Employee_ID"] not in current_employee_ids:
                row["Reason"] = "Employee ID does not exist in Master Employees."
                rejected_records.append(row.to_dict())
                continue
            # Validation 2: Skill Code must exist
            if row["Skill_Code"] not in current_skill_codes:
                row["Reason"] = "Skill Code does not exist in Master Skills."
                rejected_records.append(row.to_dict())
                continue

            # Remove old certification for the same employee/skill pair (UPDATE/OVERWRITE)
            master["Employee_Skills_Map"] = master["Employee_Skills_Map"][
                ~(
                    (master["Employee_Skills_Map"]["Employee_ID"] == row["Employee_ID"])
                    & (master["Employee_Skills_Map"]["Skill_Code"] == row["Skill_Code"])
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

    # --- Step 4: Write Master Data, Archive, and Report Rejections ---
    print("\n[STEP 4/4] Finalizing changes...")

    # Write Master DataFrames back to the single Master_Database.xlsx (4 sheets)
    try:
        with pd.ExcelWriter(MASTER_DB_PATH, engine="openpyxl") as writer:
            master["Employees"].to_excel(writer, sheet_name="Employees", index=False)
            master["Team_Data"].to_excel(writer, sheet_name="Team_Data", index=False)
            master["Skills"].to_excel(writer, sheet_name="Skills", index=False)
            master["Employee_Skills_Map"].to_excel(
                writer, sheet_name="Employee_Skills_Map", index=False
            )
        print("SUCCESS: Master Database successfully updated (4 sheets).")
    except Exception as e:
        print(f"ERROR: Failed to write to Master Database. Check file permissions: {e}")
        return

    # Handle Rejections
    if rejected_records:
        rejected_df = pd.DataFrame(rejected_records)
        rejected_path = os.path.join(
            ARCHIVE_DIR,
            f'REJECTED_updates_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx',
        )
        rejected_df.to_excel(rejected_path, index=False)
        print(
            f"WARNING: {len(rejected_df)} updates were rejected. See REJECTED file in Archive."
        )

    # Archive the processed update queue
    archive_name = f'PROCESSED_updates_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
    os.rename(UPDATE_QUEUE_PATH, os.path.join(ARCHIVE_DIR, archive_name))
    print(f"SUCCESS: Update Queue archived. ETL process finished.")


# --- PHASE 2 TESTING FUNCTION ---


def run_phase_2_test():
    """Sets up and runs a comprehensive test using the Update_Queue.xlsx."""
    print("\n--- Running Phase 2 Comprehensive Test ---")

    update_data = {
        # TRANSACTION 1: Remove Team 30 (Ops Management). Should fail validation (employee E003 is still linked).
        "Remove_Team": pd.DataFrame({"Team_ID": [30]}),
        # TRANSACTION 2: Add a new valid team (Team 40). Should succeed.
        "Add_Team": pd.DataFrame(
            {"Team_ID": [40], "Team_Name": ["New Warehouse"], "Manager": ["J. Smith"]}
        ),
        # TRANSACTION 3: Remove Skill 102 (Safety). Should succeed and cascade delete map entries for 102.
        "Remove_Skill": pd.DataFrame({"Skill_Code": [102]}),
        # TRANSACTION 4: Add new employee: E004 (valid, Team 40 added above). E999 (invalid, Team 99).
        "Add_Employee": pd.DataFrame(
            {
                "Employee_ID": ["E004", "E999"],
                "Name": ["New Hire Ops", "Invalid Hire"],
                "Team_ID": [40, 99],
                "Status": ["Active", "Active"],
            }
        ),
        # TRANSACTION 5: Add new skill 902. Should succeed.
        "Add_Skill": pd.DataFrame(
            {
                "Skill_Code": [902],
                "Skill_Name": ["Python Programming for Data Science"],
                "Category": ["Technical"],
                "Required_Ops_Area": ["IT/Ops Support"],
            }
        ),
        # TRANSACTION 6: Add Training Map
        # - Rec 1: Valid (E004 gets new skill 902). Succeeds.
        # - Rec 2: Invalid Employee ID (E999). Rejected by validation.
        # - Rec 3: Valid employee E0810 getting removed skill 102. Rejected by validation (Skill 102 is removed in Trans 3).
        "Add_Training_Map": pd.DataFrame(
            {
                "Employee_ID": ["E004", "E999", "E0810"],
                "Skill_Code": [902, 101, 102],
                "Certification_Date": [datetime.now(), datetime.now(), datetime.now()],
                "Expiration_Date": [pd.NaT, pd.NaT, pd.NaT],
                "Trainer": ["Test", "Test", "Test"],
            }
        ),
        # TRANSACTION 7: Remove Employee E1225 (Saya). Should succeed and remove map entries.
        "Remove_Employee": pd.DataFrame({"Employee_ID": ["E1225"]}),
    }

    try:
        # Create/overwrite the Update_Queue.xlsx with all sheets for testing
        with pd.ExcelWriter(UPDATE_QUEUE_PATH, engine="openpyxl") as writer:
            for sheet_name, df in update_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print("Test Update_Queue.xlsx created with 7 transaction types.")

        # Run the full ETL process
        process_all_updates()

    except Exception as e:
        print(f"ERROR: Phase 2 Test failed during setup or execution: {e}")


# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    # Ensure directories exist
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    # Forced re-initialization for consistent testing
    if os.path.exists(MASTER_DB_PATH):
        print(
            "\nMaster DB exists. Deleting and re-initializing to ensure 4-sheet structure for test."
        )
        os.remove(MASTER_DB_PATH)

    initialize_master_database()

    # Run the comprehensive test
    run_phase_2_test()
