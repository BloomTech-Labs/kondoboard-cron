from src.app.load import query
import pandas as pd


def test_query():
    exam = {
        "id": ["JS927927c0a7eb091796b82aea8f3a0770459567e4c662b9d727a428ccdeea092a"],
        "post_url": [
            "https://neuvoo.com/job.php?id=0aee8ad3da98&oapply=org_v2020-06&source=kimblegroup_bulk&utm_source=partner&utm_medium=kimblegroup_bulk&puid=gddg3aefgadb3aebfdd83aef3aec3dedbaacda9f4da7fda8aea33de83ee3fbdbgbddacdc9ed37ddf9ddbfdd7"
        ],
        "title": ["Systems Engineer, EPMS"],
        "title_keyword": ["systems engineer, epms"],
        "tags": [
            [
                "data center facilities",
                "ms access",
                "network management",
                "dns",
                "systems engineer",
                "ip",
                "hands on",
                "architecture",
                "troubleshooting",
                "monitoring",
                "communications",
                "york",
                "field",
                "project",
                "technology",
                "support",
            ]
        ],
        "company": ["albireo energy"],
        "description": [
            [
                "Albireo Energy is seeking a Systems Engineer in support of technology systems at multiple data center facilities in New York, New Jersey and Delaware.",
                "Setup and administer network management and monitoring architecture",
                "Maintain and troubleshoot field communications (Modbus RTU, Modbus TCP & TCP/IP) Ability to perform hands-on work with field equipment installations and troubleshooting",
                "Knowledge of IP, DNS, WINS, DHCP, SNMP, SMTP, FTP, HTTP protocols",
                "Familiarity with MS Project and MS Access preferred",
            ]
        ],
        "publication_date": ["2020-06-10"],
        "inserted_date": ["2020-06-10"],
        "city": ["Newark"],
        "state": ["Delaware"],
        "latitude": ["39.714507"],
        "longitude": ["-75.738715"],
    }
    df = pd.DataFrame(data=exam)
    q = query(df)
    assert q[0] == 1
