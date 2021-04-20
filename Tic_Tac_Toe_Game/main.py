from enum import Enum


class Cell(Enum):
    VOID = 0
    CROSS = 1
    ZERO = 2


class Player:
    """
    Класс играка, содержащий тип значков и имя
    """
    pass


class GameField:
    """

    """
    pass


class GameFieldView:
    """

    """
    pass


class GameRoundManager:
    """
    Менеджер игры, запускающий процессы
    """

    def __init__(self, player1: Player, player2: Player):
        self.game_over = False
        self._players = [player1, player2]
        self._current_player = 0

    def main_loop(self):
        while not self.game_over:
            player = self._players[self._current_player]


class GameWindow:
    """

    """
    pass
