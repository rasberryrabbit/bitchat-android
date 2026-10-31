package com.bitchat.android.mesh

import android.bluetooth.BluetoothAdapter
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.util.Log

/**
 * Receives BluetoothAdapter state-change broadcasts and forwards them to the active mesh
 * connection manager so that BLE scanning can be force-restarted whenever the user toggles
 * Bluetooth off and back on.
 *
 * Why this exists
 * ---------------
 * The Android BLE stack invalidates all [android.bluetooth.le.BluetoothLeScanner] callbacks
 * when the adapter is turned off. When the user turns Bluetooth back on, bitchat's
 * [BluetoothGattClientManager] would otherwise keep reporting "scanning" via its stale
 * [BluetoothGattClientManager.isCurrentlyScanning] flag, while in reality no scan is
 * running. Peers within range would silently disappear until the watchdog noticed
 * (potentially never, if scanningDesired had been cleared). This receiver closes that gap
 * by explicitly forcing a scan restart on every OFF -> ON transition.
 *
 * Lifecycle
 * ---------
 * The receiver is registered dynamically by [BluetoothConnectionManager.startServices] and
 * unregistered by [BluetoothConnectionManager.stopServices] /
 * [BluetoothConnectionManager.disableTransport], so it only fires while the mesh is alive.
 *
 * Thread-safety
 * -------------
 * [BroadcastReceiver.onReceive] is called on the main thread. The handler here only logs
 * and dispatches a single call to the connection manager, which itself schedules the real
 * work on its IO coroutine scope, so no main-thread BLE work is performed.
 */
class BluetoothStateReceiver(
    private val onStateChanged: (state: Int) -> Unit
) : BroadcastReceiver() {

    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action != BluetoothAdapter.ACTION_STATE_CHANGED) return

        val state = intent.getIntExtra(BluetoothAdapter.EXTRA_STATE, BluetoothAdapter.ERROR)
        val previousState = intent.getIntExtra(
            BluetoothAdapter.EXTRA_PREVIOUS_STATE,
            BluetoothAdapter.ERROR
        )

        Log.i(
            TAG,
            "Bluetooth adapter state changed: $previousState -> $state" +
                " (${stateToString(previousState)} -> ${stateToString(state)})"
        )

        try {
            onStateChanged(state)
        } catch (e: Exception) {
            Log.e(TAG, "Failed to dispatch Bluetooth state change: ${e.message}", e)
        }
    }

    companion object {
        private const val TAG = "BluetoothStateReceiver"

        /**
         * Human-readable name for an [BluetoothAdapter] state constant, for log lines.
         */
        fun stateToString(state: Int): String = when (state) {
            BluetoothAdapter.STATE_OFF -> "OFF"
            BluetoothAdapter.STATE_TURNING_OFF -> "TURNING_OFF"
            BluetoothAdapter.STATE_ON -> "ON"
            BluetoothAdapter.STATE_TURNING_ON -> "TURNING_ON"
            BluetoothAdapter.STATE_CONNECTING -> "CONNECTING"
            BluetoothAdapter.STATE_DISCONNECTED -> "DISCONNECTED"
            else -> if (state == BluetoothAdapter.ERROR) "ERROR" else "UNKNOWN($state)"
        }
    }
}
