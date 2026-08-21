import os

with open('generate_dashboard_jenkins_local.py', 'r', encoding='utf-8') as f:
    local_content = f.read()

with open('generate_dashboard_jenkins.py', 'r', encoding='utf-8') as f:
    jenkins_content = f.read()

# The DB logic is at the top of Jenkins (ends just before `def process_data`).
start_idx_jenkins = jenkins_content.find("def process_data(df: pd.DataFrame) -> dict:")
start_idx_local = local_content.find("def process_data(df: pd.DataFrame) -> dict:")

# The Github logic is at the bottom of Jenkins (starts with `# ---------------------------------------------------------------------------\n# GitHub Integration`).
# If this string is not found, we can look for `def push_to_github`
end_idx_jenkins = jenkins_content.find("def push_to_github")
if end_idx_jenkins != -1:
    end_idx_jenkins = jenkins_content.rfind("# ---------------------------------------------------------------------------", 0, end_idx_jenkins)

# The local logic stops at `# Main`
end_idx_local = local_content.find("def main():")
if end_idx_local != -1:
    end_idx_local = local_content.rfind("# ---------------------------------------------------------------------------", 0, end_idx_local)

print(f"start_idx_jenkins: {start_idx_jenkins}")
print(f"start_idx_local: {start_idx_local}")
print(f"end_idx_jenkins: {end_idx_jenkins}")
print(f"end_idx_local: {end_idx_local}")

if start_idx_jenkins != -1 and start_idx_local != -1 and end_idx_jenkins != -1 and end_idx_local != -1:
    new_jenkins = jenkins_content[:start_idx_jenkins] + local_content[start_idx_local:end_idx_local] + jenkins_content[end_idx_jenkins:]
    with open('generate_dashboard_jenkins.py', 'w', encoding='utf-8') as f:
        f.write(new_jenkins)
    print("SUCCESSFULLY MERGED!")
else:
    print("FAILED TO MERGE")
