import bot2
src = open(bot2.__file__, encoding='utf-8').read()
print('FILE PYTHON LOADS:', bot2.__file__)
print('Mean-reversion count:', src.count('Mean-reversion'))
print('Reversion count:', src.count('"Reversion"'))
print('waves_since_choc count:', src.count('waves_since_choc'))
