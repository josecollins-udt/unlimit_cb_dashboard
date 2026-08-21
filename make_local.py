import re
import os

target_dir = r"c:\Users\Undostres Collins\Documents\code\Python\unlimit_CB_report_dashboard"
jenkins_file = os.path.join(target_dir, "generate_dashboard_jenkins.py")
out_file = os.path.join(target_dir, "generate_dashboard_jenkins_local.py")

with open(jenkins_file, 'r', encoding='utf-8') as f:
    content = f.read()

# Remove github token stuff
content = re.sub(r'(?s)# ---------------------------------------------------------------------------\n# Github data\n# ---------------------------------------------------------------------------.*?GITHUB_FILE_PATH = \'dashboard_output\.html\'\nCOMMIT_MESSAGE = [^\n]+\n', '', content)

# Remove database connection using env vars
content = re.sub(r'(?s)# ---------------------------------------------------------------------------\n# Database connection\n# ---------------------------------------------------------------------------.*?sys\.stdout\.flush\(\)\n', 'from db_connection import get_db_connection\n', content)

# Remove Github authentication
content = re.sub(r'(?s)# ---------------------------------------------------------------------------\n# Github authentication\n# ---------------------------------------------------------------------------.*?repo = github\.get_user\(GITHUB_USER\)\.get_repo\(GITHUB_REPO\)\n', '', content)

# Fix fetch_data to use get_db_connection()
fetch_data_replacement = '''def fetch_data():
    """Fetch all chargeback records using a single unified query."""
    conn = get_db_connection()
    if conn is None:
        raise ConnectionError("Could not connect to the database.")
    try:
        query = """
        SELECT
          cbs.user_id, 
          cbs.amount, 
          cbs.operator, 
          cbs.credit_card, 
          bl.type, 
          bl.standard_bank_name AS bank, 
          bl.country, 
          cbs.payment_date, 
          cbs.chargeback_received_date, 
          COALESCE(cbs.transaction_status,0) AS transaction_status,
          IF(fcbs.sift_id IS NOT NULL, 1, 0) AS is_fought, 
          fcbs.status, 
          fcbs.created_at AS submission_date, 
          fcbs.result_date
        FROM fraud.cb_payments AS cbs
        LEFT JOIN saldogra_gamma.binlist AS bl
        ON LEFT(cbs.credit_card,6) = bl.card_first_6
        LEFT JOIN fraud.fought_cbs_followup AS fcbs
        ON cbs.id = fcbs.payment_id
        WHERE cbs.chargeback_received_date > DATE_FORMAT(CURDATE() - INTERVAL 6 MONTH, '%Y-%m-01')
        AND cbs.rechargeApi IN (9,11)
        """
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()'''

content = re.sub(r'(?s)def fetch_data\(\):.*?return df.*?except Exception as e:.*?raise\n', fetch_data_replacement + '\n', content)

# Remove Github integration function
content = re.sub(r'(?s)# ---------------------------------------------------------------------------\n# GitHub Integration\n# ---------------------------------------------------------------------------.*?def push_to_github\(file_path\):.*?print\(f"      -> ERROR during Git operations: \{e\}"\)\n', '', content)

# Remove push_to_github from main
content = re.sub(r'    # Automate github submission\n    push_to_github\(out_path\)\n', '', content)

# Also rename out_path to dashboard_output_jenkins_local.html
content = re.sub(r'out_path = os\.path\.join\(os\.path\.dirname\(__file__\), "dashboard_output\.html"\)', 'out_path = os.path.join(os.path.dirname(__file__), "dashboard_output_jenkins_local.html")', content)

with open(out_file, 'w', encoding='utf-8') as f:
    f.write(content)

print(f"Created {out_file}")
