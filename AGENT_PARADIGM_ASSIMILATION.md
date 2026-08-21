# Agent Protocol: Knowledge Base Stratification & Workspace Paradigm Assimilation

**To the executing Agent:** 
You have been explicitly directed to this document to assimilate the "Knowledge Base Stratification" and "Workspace Reorganization" paradigm into the current project. This is a highly optimized methodology designed to drastically reduce cognitive load, improve execution efficiency, and maintain a pristine workspace.

**IMMEDIATE DIRECTIVE:** Read this entire document carefully. Do not perform any other tasks until you have developed an `implementation_plan.md` to assimilate this project to the paradigm described below. Once assimilation is complete and approved by the user, you MUST delete this file.

---

## The Paradigm: Knowledge Base Stratification

The core philosophy is that the root directory and primary context windows must remain uncluttered. High-level business logic is kept in primary documents, while deeply technical, line-by-line reference material is pushed to an `appendix/`. Code is strictly compartmentalized by its domain.

### Key Principles
1. **Stratified Documentation:**
   - **Primary Docs (`docs/`)**: High-level overviews (`01_ARCHITECTURE.md`, `02_DATA_DICTIONARY.md`, `03_RUNBOOK.md`).
   - **Appendices (`docs/appendix/`)**: Exhaustive technical references (`01_DATABASE_SCHEMA.md` with exact `SHOW CREATE TABLE` definitions, `02_ENVIRONMENT_SPECIFICATIONS.md` detailing credentials/CI-CD configs). 
   - *Rule*: Primary docs must always contain callouts (`> [!NOTE]`) linking to their respective detailed appendices.
   - *Rule*: A similar documentation approach MUST be kept regarding scripts. Complex operational logic or CI/CD pipelines must be thoroughly detailed in their own respective appendices.

2. **Workspace Compartmentalization (Folder Stratification):**
   - The root directory must contain ONLY essential configuration files (e.g., `preamble.py`, `.env`, `.gitignore`).
   - All code must be moved into a `src/` directory, sub-divided by domain (e.g., `src/production_pipeline/`, `src/analytics/`, `src/maintenance/`).
   - If applicable, each domain must be further sub-divided into execution environments (e.g., `local/` vs `jenkins_compatible/` or `prod/`).
   - All non-code assets (`.csv`, `.json`, `.png`) MUST be moved to a `data_and_benchmarks/` quarantine zone, cleanly sub-divided by filetype.
   - Deprecated or historical scripts must be moved to `legacy_tasks/`.
   - **Reference Integrity:** Whenever scripts and files are reorganized, you MUST update all internal references, file paths, and import links within the codebase so that all scripts remain fully functional.

3. **Dependency Documentation:**
   - As part of the stratification, you MUST evaluate and explicitly document the interdependencies between scripts, data files, and environment variables. This map of dependencies should reside in an appendix (e.g., `docs/appendix/03_INTERDEPENDENCIES.md`) so agents understand the blast radius of modifications.

4. **File Permission Workarounds:**
   - If you encounter Windows ACL or Permission Denied errors when trying to reorganize files via bash/internal commands (`mkdir`, `mv`), you MUST generate a Python script (e.g., `reorganize_workspace.py`) using `shutil` and `os.makedirs`, and explicitly ask the user to execute it on your behalf.

---

## Assimilation Execution Workflows

Depending on the current state of this project, execute one of the following two workflows:

### Scenario 1: No Existing Context Architecture
If the project lacks an established `.agents/AGENTS.md` or `docs/` structure:
1. **Analyze:** Scan the entire root directory.
2. **Plan:** Create an `implementation_plan.md`. Propose a `src/` folder hierarchy tailored to the project's scripts. Propose the creation of `docs/01_ARCHITECTURE.md`, `docs/02_DATA_DICTIONARY.md`, and `docs/appendix/01_DATABASE_SCHEMA.md`.
3. **Execute:** 
   - Generate a reorganization Python script (if needed for permissions) to move the files.
   - Create the documentation files. 
   - Generate an `.agents/AGENTS.md` file enshrining the Knowledge Base Stratification rules.

### Scenario 2: Existing Context Files
If the project already has `.agents/AGENTS.md` and/or some `docs/`:
1. **Analyze & Compare:** Read the existing `AGENTS.md` and documentation. 
2. **Conflict Resolution:** If any existing instructions conflict with the Stratification Paradigm, you MUST STOP and document these conflicts in an `implementation_plan.md`. Explicitly ask the user how to resolve them before modifying any rules.
3. **Plan:** Propose a directory reorganization and the creation/migration of deeply technical data into `docs/appendix/`.
4. **Execute:** 
   - Generate the reorganization script.
   - Update `AGENTS.md` to append the Knowledge Base Stratification and File Permission Workaround rules.
   - Update primary docs to link to new appendices.

---
**Final Instruction to Agent:**
Once you have successfully executed the assimilation plan, confirm with the user. Upon their final approval of the new structure, **delete this file (`AGENT_PARADIGM_ASSIMILATION.md`)** to conclude the assimilation process.
