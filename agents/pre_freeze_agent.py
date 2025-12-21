# %% 📚 Imports
import pandas as pd
from core.phase1_clean import phase1_load_and_clean_raw_v2
from core.phase2_parse import phase2_parse_symbols
from core.phase3_pcs_score import phase3_score_pcs
from core.phase4_snapshot import phase4_save_snapshot

# %% 🧠 Pre-Freeze Pipeline Agent
class PreFreezeAgent:
    def __init__(self, input_path: str, snapshot_dir: str):
        self.input_path = input_path
        self.snapshot_dir = snapshot_dir
        self.df = pd.DataFrame()
        self.snapshot_path = ""

    def run_pipeline(self):
        print("🚀 PreFreezeAgent starting...")
        self.df = phase1_load_and_clean_raw_v2(
            input_path=self.input_path,
            snapshot_dir=self.snapshot_dir
        )

        self.df = phase2_parse_symbols(self.df)
        self.df = phase3_score_pcs(self.df)
        self.snapshot_path = phase4_save_snapshot(self.df)

        print("✅ PreFreezeAgent completed.")
        return self.df, self.snapshot_path

# %% 🧪 Run Standalone
if __name__ == "__main__":
    agent = PreFreezeAgent(
        input_path="/Users/haniabadi/Documents/Windows/Positions_Account_.csv",
        snapshot_dir="/Users/haniabadi/Documents/Windows/Optionrec/drift"
    )
    df_final, path = agent.run_pipeline()
    print("📁 Snapshot saved to:", path)
    print("🔍 Final df.head():")
    print(df_final.head())
