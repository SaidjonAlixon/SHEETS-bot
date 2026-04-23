from aiogram.fsm.state import StatesGroup, State

class BotStates(StatesGroup):
    AdminAddAccess = State()
    AdminRevokeAccess = State()
    AdminAddAdmin = State()
    AdminRevokeAdmin = State()
    Factoring = State()
    FactoringDateRange = State()
    FactoringSearchLoad = State()  # Yuk raqami kutilmoqda
    Broker = State()
    Fuel = State()
    FuelSheetSelect = State()  # Fuel filedan keyin qaysi listni tanlash
    FuelRangeSelect = State()  # Fuel sheet ichidagi sana oralig'ini tanlash
    Toll = State()
    TollSheetSelect = State()
    Statement = State()
    StatementCompanyDriverPdf = State()
    StatementOwnerOperatorPdf = State()
    StatementContractorPdf = State()
