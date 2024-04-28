import os
import json
import pandas as pd
import numpy as np
import chess.engine
from chess import pgn


def get_engine_stockfish(path):
    stock_params = { "Threads": 3, "Hash": 2048, "Skill Level": 20, "UCI_LimitStrength": "false" }
    engine = chess.engine.SimpleEngine.popen_uci(path)
    engine.configure(stock_params)
    return engine

def get_eval(score, board):
    if score.is_mate():
        if board.is_checkmate():
            return -8795 if board.turn == chess.WHITE else 8795
        else:
            mate_in = score.pov(board.turn).mate()
            return 8795 if mate_in > 0 else -8795
    else:
        return score.white().score()

def get_winning_chance(score, board):
    if score.is_mate():
        mate_in = score.pov(board.turn).mate()
        return 1 if mate_in > 0 else 0
    else:
        return score.white().wdl().winning_chance()

def get_losing_chance(score, board):
    if score.is_mate():
        mate_in = score.pov(board.turn).mate()
        return 0 if mate_in > 0 else 1
    else:
        return score.white().wdl().losing_chance()

def fen_metrics(fen, engine):
    board = chess.Board(fen)
    info = engine.analyse(board, chess.engine.Limit(depth=6))
    score = info["score"]

    eval = get_eval(score, board)
    winning_chance = get_winning_chance(score, board)
    losing_chance = get_losing_chance(score, board)

    eval_replaced = 1 if score.is_mate() else 0

    return (eval, eval_replaced, winning_chance, losing_chance)


def game_metrics(fen_list, engine):
    metrics = [fen_metrics(fen, engine) for fen in fen_list]
    return list(zip(*metrics))  # Unzips the metrics into separate lists


def load_json(df, cols):
    for col in cols:
        df[col] = df[col].apply(lambda row: json.loads(row))
    return df
