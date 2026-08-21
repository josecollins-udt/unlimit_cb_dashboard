import os

with open('generate_dashboard_jenkins_local.py', 'r', encoding='utf-8') as f:
    local_content = f.read()

with open('generate_dashboard_jenkins.py', 'r', encoding='utf-8') as f:
    jenkins_content = f.read()

# In local, we want to grab everything starting from `_aggregate_dashboard_metrics`
start_idx_local = local_content.find("def _aggregate_dashboard_metrics(df: pd.DataFrame) -> dict:")
end_idx_local = local_content.find("def main():")
if end_idx_local != -1:
    end_idx_local = local_content.rfind("# ---------------------------------------------------------------------------", 0, end_idx_local)

# In jenkins, we want to replace everything starting from `process_data`
start_idx_jenkins = jenkins_content.find("def process_data(df: pd.DataFrame) -> dict:")

# And end before push_to_github
end_idx_jenkins = jenkins_content.find("def push_to_github")
if end_idx_jenkins != -1:
    end_idx_jenkins = jenkins_content.rfind("# ---------------------------------------------------------------------------", 0, end_idx_jenkins)

if start_idx_jenkins != -1 and start_idx_local != -1 and end_idx_jenkins != -1 and end_idx_local != -1:
    new_jenkins = jenkins_content[:start_idx_jenkins] + local_content[start_idx_local:end_idx_local] + jenkins_content[end_idx_jenkins:]
    with open('generate_dashboard_jenkins.py', 'w', encoding='utf-8') as f:
        f.write(new_jenkins)
    print("SUCCESSFULLY MERGED! _aggregate_dashboard_metrics is now fully restored.")
else:
    print("FAILED TO MERGE: Could not find one of the section boundaries.")
