"""
modules/cli/tool/diagnose.py - 数据源诊断
"""


def cmd_diagnose(args):
    from scripts.diagnose_crawler import run_diagnose
    print(run_diagnose(source=args.source, as_json=args.as_json, fresh=args.fresh))
