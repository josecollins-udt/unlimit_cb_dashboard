import os

with open('generate_dashboard_jenkins_local.py', 'r', encoding='utf-8') as f:
    local_content = f.read()

with open('generate_dashboard_jenkins.py', 'r', encoding='utf-8') as f:
    jenkins_content = f.read()

start_local = local_content.find('def process_data(df: pd.DataFrame) -> dict:')
end_local = local_content.find('# ---------------------------------------------------------------------------\n# Main')

start_jenkins = jenkins_content.find('def process_data(df: pd.DataFrame) -> dict:')
end_jenkins = jenkins_content.find('# ---------------------------------------------------------------------------\n# GitHub Integration')

if start_local != -1 and end_local != -1 and start_jenkins != -1 and end_jenkins != -1:
    local_core = local_content[start_local:end_local]
    new_jenkins = jenkins_content[:start_jenkins] + local_core + jenkins_content[end_jenkins:]
    with open('generate_dashboard_jenkins.py', 'w', encoding='utf-8') as f:
        f.write(new_jenkins)
    print('Merged successfully!')
else:
    print('Failed to find indices')
    print('local', start_local, end_local)
    print('jenkins', start_jenkins, end_jenkins)
