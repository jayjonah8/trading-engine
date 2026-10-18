# Trading Engine Lifecycle

## Overview

The trading engine is the authoritative owner of trading state.
Strategies express **intent**, execution adapters communicate with
venues, and market-data providers publish normalized market events. The
engine sits between these systems and decides when state is allowed to
change.

The core separation is:

- **TradeIntent** --- what exposure a strategy wants and how the
  initial entry should be executed.
- **Trade** --- the logical strategy-level lifecycle and risk plan.
- **Order** --- an execution request created by the engine.
- **Fill** --- a confirmed execution fact reported by a venue or
  simulator.
- **Position** --- the engine's view of actual venue/account exposure.
- **RiskReservation / RiskState** --- reserved and established risk
  capacity.
- **EngineView** --- read-only state published back to strategies.

```text
Market Data
    |
    v
AppEventEnvelope
    |
    v
Strategy
    |
    | TradeIntent
    v
Engine
    |
    +--> Trade
    +--> Risk Check / Reservation
    +--> Order
    |
    | ExecCommand
    v
Execution Adapter
    |
    v
Venue / Simulator
    |
    | ExecEvent
    v
Engine
    |
    +--> Orders
    +--> Fills
    +--> Positions
    +--> Trades
    +--> RiskState
    |
    v
EngineView
    |
    v
Strategies
```

---

## 1. Market Data Enters the Application

Live market-data adapters such as Alpaca or Hyperliquid translate
venue-specific messages into canonical application events.

Examples include:

```text
TapeTrade
Quote
BarClosed
```

These are wrapped in an `AppEventEnvelope` and published to the
application event bus.

```text
Alpaca / Hyperliquid / Future Provider
                |
                v
       Venue-specific message
                |
                v
        Normalize market data
                |
                v
       AppEventEnvelope
                |
                v
          Strategy bus
```

The strategy should not need to know whether a quote came from Alpaca,
Hyperliquid, or another provider.

---

## 2. Strategy Receives Market Events

A strategy consumes canonical `AppEventEnvelope` events and makes
decisions using market data plus the latest `EngineView`.

For example:

```text
Quote
  |
  +--> calculate midpoint
  +--> update rolling window
  +--> check trading session
  +--> inspect EngineView
  +--> check existing active trade
  |
  v
Trading signal?
```

Before creating another directional trade for a symbol, a strategy can
check:

```rust
view.trades.has_active_trade_for_symbol(&ev.symbol)
```

An active trade includes:

```text
Opening
Open
Closing
```

A `Closed` trade is terminal and no longer blocks a new trade.

---

## 3. TradeIntent

When a strategy decides to trade, it creates a canonical `TradeIntent`.

A `TradeIntent` describes the desired trade, not an exchange order.

It contains information such as:

```text
strategy_id
symbol
direction
signal_px
target_notional

order_type
time_in_force
limit_px
stop_px
post_only

take_profit_percent
stop_loss_percent
emergency_stop_percent
stop_loss_delay_secs
trade_duration_secs

ts_ms
```

The strategy then sends:

```text
EngineEvent::OpenTrade(TradeIntent)
```

Conceptually:

```text
Strategy Signal
      |
      v
 TradeIntent
      |
      v
 EngineEvent::OpenTrade
      |
      v
    Engine
```

---

## 4. Engine Validates the Intent

The engine receives the intent through its authoritative event loop.

Before execution, the intent should be validated:

```text
TradeIntent
    |
    +--> valid strategy ID?
    +--> valid symbol?
    +--> valid timestamp?
    +--> valid signal price?
    +--> valid notional?
    +--> valid OrderType configuration?
    +--> valid limit / stop prices?
    +--> valid risk percentages?
    |
    v
Accepted / Rejected
```

Malformed strategy requests should never create authoritative trade or
order state.

---

## 5. Trade Creation

Once accepted, the engine creates a logical `Trade`.

The trade begins in:

```text
TradeState::Opening
```

This is important: `Opening` means the strategy trade lifecycle already
exists even though its intended exposure may not yet be fully
established.

```text
TradeIntent accepted
        |
        v
   Trade created
        |
        v
TradeState::Opening
```

The `Trade` owns strategy-level information such as:

```text
TradeId
strategy_id
symbol
direction
quantity
entry / exit average prices
signal prices
target notional
risk plan
timestamps
TradeState
```

It does **not** own the exchange order lifecycle.

---

## 6. TradeState Lifecycle

The logical trade lifecycle is:

```text
              entry begins
                   |
                   v
              +---------+
              | Opening |
              +---------+
                   |
          exposure established
                   |
                   v
              +---------+
              |  Open   |
              +---------+
                   |
             exit requested
                   |
                   v
              +---------+
              | Closing |
              +---------+
                   |
          remaining exposure = 0
                   |
                   v
              +---------+
              | Closed  |
              +---------+
```

### Opening

The trade exists and an entry is being worked, but the intended position
has not yet been fully established.

### Open

The trade has established exposure and is actively held.

### Closing

An exit is being worked, but exposure still remains. The trade remains
active until confirmed fills reduce its remaining exposure to zero.

### Closed

No exposure remains for the trade. This is the terminal state.

`TradeState::is_active()` therefore returns true for:

```text
Opening
Open
Closing
```

and false for:

```text
Closed
```

---

## 7. Risk Check and Reservation

Before an order is allowed to consume additional exposure, the engine
checks authoritative risk state.

This is separate from simply counting trades.

```text
Trade
  |
  v
Risk Check
  |
  +--> max active trades
  +--> max order notional
  +--> max gross notional
  +--> max symbol notional
  +--> available account capacity
  +--> existing reservations
  |
  v
RiskReservation
```

`Trades::count_active()` is useful for lifecycle accounting, but
`RiskState` is authoritative for production risk capacity because it can
account for reserved exposure from working orders.

This prevents asynchronous execution from accidentally overcommitting
capital.

---

## 8. Engine Creates an Order

After the trade and risk checks are accepted, the engine creates an
`Order`.

The order is execution state and remains separate from the trade.

```text
Trade
  |
  +--> Entry Order #1
  |
  +--> Replacement Order
  |
  +--> Exit Order #2
  |
  +--> Emergency Exit Order
```

One trade can therefore own or reference multiple orders over its
lifetime.

An order contains execution concepts such as:

```text
OrderId
TradeId
symbol
side
quantity
filled quantity
OrderType
TimeInForce
limit price
stop price
OrderState
venue order ID
```

This separation is essential for limit orders, partial fills,
cancel/replace flows, and future market-making support.

---

## 9. ExecCommand Leaves the Engine

The engine does not directly call Alpaca, IBKR, Hyperliquid, or another
exchange API.

Instead it emits a venue-independent `ExecCommand`.

```text
Engine
  |
  | ExecCommand
  v
Execution Adapter
```

Examples conceptually include:

```text
SubmitOrder
CancelOrder
ReplaceOrder
```

The configured execution adapter translates the canonical command into
whatever API format the venue requires.

```text
                     +--> Alpaca
                     |
Engine -> ExecCommand +--> IBKR
                     |
                     +--> Hyperliquid
                     |
                     +--> Backtest Simulator
```

The engine remains venue-independent.

---

## 10. Execution Adapter

The execution adapter owns communication with the venue.

Its responsibilities include:

```text
REST / WebSocket / broker API communication
order submission
cancel requests
replace requests
execution-update connection
venue-specific parsing
reconnection
mapping venue updates -> canonical ExecEvent
```

It does **not** authoritatively mutate engine trade state.

For example:

```text
Engine
   |
   | SubmitOrder
   v
Alpaca Adapter
   |
   | Alpaca REST
   v
Alpaca
   |
   | trade_updates WebSocket
   v
Alpaca Adapter
   |
   | canonical ExecEvent
   v
Engine
```

---

## 11. ExecEvent Returns to the Engine

Execution adapters report execution facts using canonical `ExecEvent`s.

The engine currently models events such as:

```text
OrderAccepted
OrderWorking
Fill
OrderCanceled
OrderExpired
OrderRejected
OrderReplaced
Error
```

The critical distinction is:

```text
ExecCommand = request

ExecEvent = confirmed execution fact
```

Sending an order does not mean it filled.

---

## 12. Partial Fill Lifecycle

Partial fills are first-class execution events.

Example:

```text
TradeState::Opening

Entry Order
qty = 100
    |
    +--> Fill 25
    |      remaining = 75
    |
    +--> Fill 30
    |      remaining = 45
    |
    +--> Fill 45
           remaining = 0
```

Each fill updates authoritative state.

Conceptually:

```text
ExecEvent::Fill
       |
       +--> FillLedger
       +--> Order filled quantity
       +--> Trade quantity / average price
       +--> Position exposure
       +--> RiskState
       |
       v
Remaining entry complete?
```

When the entry reaches the engine's definition of established exposure,
the logical trade transitions appropriately from `Opening` toward
`Open`.

The exact transition policy should be defined centrally by the execution
handlers rather than by the strategy.

---

## 13. Position State

`Position` and `Trade` answer different questions.

```text
Trade
"What strategy lifecycle does this exposure belong to?"

Position
"What exposure does the account actually have?"
```

For example:

```text
Strategy A Trade
    +100 AAPL

Strategy B Trade
     +50 AAPL

----------------
Venue Position
    +150 AAPL
```

This separation becomes increasingly important with multiple strategies
and multiple venues.

---

## 14. EngineView

After authoritative state changes, the engine publishes a read-only
`EngineView`.

```text
Engine State
    |
    v
publish_view()
    |
    v
watch::Sender<Arc<EngineView>>
    |
    +--> Strategy A
    +--> Strategy B
    +--> Strategy C
```

Strategies inspect the view but do not directly mutate engine state.

This maintains the rule:

```text
Strategies request.
Adapters report.
Engine decides and mutates.
EngineView exposes.
```

---

## 15. Closing a Trade

A close request does not immediately mean the trade is closed.

```text
TradeState::Open
       |
       | exit condition
       v
Engine close request
       |
       v
TradeState::Closing
       |
       v
Exit Order
       |
       v
ExecCommand
       |
       v
Venue
       |
       +--> partial exit fills
       |
       +--> final exit fill
                |
                v
        remaining exposure = 0
                |
                v
        TradeState::Closed
```

`Closing` remains an active trade state because exposure still exists
until confirmed execution removes it.

---

## 16. Cancel and Replace

Orders can be canceled or modified independently of the logical trade.

```text
Trade
  |
  v
Working Limit Order
  |
  +--> CancelOrder
  |
  +--> ReplaceOrder
          |
          +--> quantity
          +--> limit_px
          +--> stop_px
```

The engine should not assume success when sending these requests.

```text
Engine request
     |
     v
Pending execution state
     |
     v
Adapter / Venue
     |
     v
ExecEvent confirmation
     |
     v
Authoritative state transition
```

This asynchronous model is necessary for real venues.

---

## 17. Backtesting Uses the Same Core Lifecycle

The long-term architecture should avoid maintaining a second backtest
trading engine.

Instead:

```text
LIVE

Strategy
   |
TradeIntent
   |
Engine
   |
ExecCommand
   |
Live Adapter
   |
Venue
   |
ExecEvent
   |
Engine
```

and:

```text
BACKTEST

Strategy
   |
TradeIntent
   |
Engine
   |
ExecCommand
   |
Historical Execution Simulator
   |
Simulated ExecEvent
   |
Engine
```

The engine sees the same canonical execution lifecycle in both modes.

Only the execution implementation changes.

This gives much stronger live/backtest parity for:

```text
market orders
limit orders
partial fills
cancellations
replacements
fees
slippage
execution timing
risk accounting
```

---

## 18. Engine Operating State

The engine also maintains high-level runtime state separate from
individual trades and orders.

### ExecutionMode

The execution mode determines which execution environment is active.

Conceptually:

```text
Live
Simulation
Backtest
```

Core state-transition logic should remain as shared as possible across
these modes.

### TradingPhase

`TradingPhase` represents the engine's broader operating phase.

Currently:

```text
TradingPhase::Normal
```

It can later expand to support states such as:

```text
Starting
Reconciling
Normal
ClosingTrades
ShuttingDown
Recovering
```

without mixing engine-wide operating state with `TradeState` or
`OrderState`.

---

## 19. State Ownership

The most important architectural rule is that each state model has one
clear responsibility.

```text
+------------------------------------------------------+
| ENGINE                                               |
|                                                      |
|  Trades                                              |
|  └─ strategy lifecycle                              |
|                                                      |
|  Orders                                              |
|  └─ execution request lifecycle                     |
|                                                      |
|  FillLedger                                          |
|  └─ immutable/confirmed execution facts             |
|                                                      |
|  Positions                                           |
|  └─ actual established exposure                     |
|                                                      |
|  RiskState / RiskReservations                        |
|  └─ established + reserved risk capacity            |
|                                                      |
|  General                                             |
|  └─ account / PnL / general engine information      |
+------------------------------------------------------+
              ^
              |
      authoritative mutation
              |
        Engine handlers
```

External systems should never bypass these boundaries and directly
modify authoritative state.

---

## 20. Complete Trade Lifecycle

Putting everything together:

```text
MARKET DATA
    |
    v
TapeTrade / Quote / BarClosed
    |
    v
AppEventEnvelope
    |
    v
STRATEGY
    |
    +--> EngineView check
    |
    +--> signal detected
    |
    v
TradeIntent
    |
    v
EngineEvent::OpenTrade
    |
    v
ENGINE
    |
    +--> validate TradeIntent
    |
    +--> create Trade
    |       state = Opening
    |
    +--> risk check
    |
    +--> reserve risk
    |
    +--> create entry Order
    |
    v
ExecCommand
    |
    v
EXECUTION ADAPTER
    |
    v
VENUE / SIMULATOR
    |
    +--> accepted
    +--> working
    +--> partial fill
    +--> fill
    +--> canceled
    +--> rejected
    +--> replaced
    |
    v
ExecEvent
    |
    v
ENGINE EXEC HANDLER
    |
    +--> update Order
    +--> record Fill
    +--> update Position
    +--> update Trade
    +--> update RiskState
    |
    v
TradeState::Open
    |
    +--> strategy/risk monitoring
    |
    +--> TP / SL / emergency SL / duration / strategy exit
    |
    v
Close request
    |
    v
TradeState::Closing
    |
    v
Exit Order
    |
    v
ExecCommand
    |
    v
EXECUTION ADAPTER
    |
    v
VENUE / SIMULATOR
    |
    v
Exit Fill(s)
    |
    v
ENGINE
    |
    +--> update FillLedger
    +--> reduce Position
    +--> update realized PnL
    +--> release risk
    +--> finalize Trade
    |
    v
TradeState::Closed
    |
    v
EngineView published
```

---

## Core Principle

The architecture can be summarized as:

```text
Strategy decides WHAT it wants.

Engine decides WHETHER it is allowed
and owns the authoritative lifecycle.

Order describes WHAT should be executed.

Adapter decides HOW to communicate that request
to a specific venue.

Venue tells us WHAT ACTUALLY HAPPENED.

ExecEvent brings that fact back to the engine.

Engine updates Trades, Orders, Fills, Positions,
RiskState, PnL, and EngineView.
```

This separation allows the same core engine to support live trading,
historical simulation, multiple execution venues, market and limit
orders, partial fills, asynchronous cancel/replace flows, and
increasingly sophisticated strategies without making strategy code
venue-specific.
