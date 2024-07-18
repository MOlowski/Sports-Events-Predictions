import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
from fastapi.responses import StreamingResponse
import pandas as pd

def plot_accuracy(df, ax, title, is_by_match):
    if is_by_match:
        hue_par = 'match_result'
    else:
        hue_par = 'bet_result'
    sns.barplot(x='bet_name', y='num', hue=hue_par, data=df, ax=ax)
    ax.set_title(title)
    ax.set_ylabel('Count')
    ax.set_xlabel('Bet Name')
    plt.xticks(rotation=45)
    plt.tight_layout()

def plot_incomes(incomes, ax, title):
    incomes.plot(kind='bar', ax=ax)
    ax.set_title(title)
    ax.set_ylabel('Incomes')
    plt.tight_layout()

def combine_plots(
    accuracy_data_last_weekend, 
    match_accuracy_data_last_weekend,
    incomes_last_weekend, 
    accuracy_data_all_time, 
    match_accuracy_data_all_time,
    incomes_all_time
    ):
    fig, axs = plt.subplots(3, 2, figsize=(25, 25))
    plot_accuracy(accuracy_data_last_weekend, axs[0, 0], "Bet Accuracy by Bet Name (Last Weekend)", False)
    plot_accuracy(match_accuracy_data_last_weekend, axs[1, 0], "Match Bet Accuracy by Bet Name (Last Weekend)", True)
    plot_incomes(incomes_last_weekend, axs[2, 0], "Bet incomes (Last Weekend)")
    plot_accuracy(accuracy_data_all_time, axs[0, 1], "Bet Accuracy by Bet Name (All Time)", False)
    plot_accuracy(match_accuracy_data_all_time, axs[1, 1], "Match Bet Accuracy by Bet Name (All Time)", True)
    plot_incomes(incomes_all_time, axs[2, 1], "Bet incomes (All Time)")

    plt.tight_layout()

    buf = BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf

async def get_combined_plot(accuracy_data_last_weekend, match_accuracy_data_last_weekend, incomes_last_weekend, accuracy_data_all_time, match_accuracy_data_all_time, incomes_all_time):
    buf = combine_plots(accuracy_data_last_weekend, match_accuracy_data_last_weekend, incomes_last_weekend, accuracy_data_all_time, match_accuracy_data_all_time, incomes_all_time)
    return StreamingResponse(buf, media_type="image/png")

