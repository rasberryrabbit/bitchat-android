package com.bitchat.android.mesh

import android.bluetooth.*
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.Build
import android.util.Log
import androidx.core.content.ContextCompat
import com.bitchat.android.model.RoutedPacket
import com.bitchat.android.protocol.BitchatPacket
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.combine

/**
 * Power-optimized Bluetooth connection manager with comprehensive memory management
 * Integrates with PowerManager for adaptive power consumption
 * Coordinates smaller, focused components for better maintainability
 */
class BluetoothConnectionManager(
    private val context: Context, 
    private val myPeerID: String,
    private val fragmentManager: FragmentManager? = null
) {
    
    companion object {
        private const val TAG = "BluetoothConnectionManager"
    }
    
    // Core Bluetooth components
    private val bluetoothManager: BluetoothManager = 
        context.getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager
    private val bluetoothAdapter: BluetoothAdapter? = bluetoothManager.adapter
    
    // Power management
    private val powerManager = PowerManager.getInstance(context.applicationContext)
    
    // Coroutines
    private val connectionScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    
    // Component managers
    private val permissionManager = BluetoothPermissionManager(context)
    private val connectionTracker = BluetoothConnectionTracker(connectionScope, powerManager)
    private val packetBroadcaster = BluetoothPacketBroadcaster(connectionScope, connectionTracker, fragmentManager, myPeerID)
    
    // Delegate for component managers to call back to main manager
    private val componentDelegate = object : BluetoothConnectionManagerDelegate {
        override fun onPacketReceived(
            packet: BitchatPacket,
            peerID: String,
            device: BluetoothDevice?,
            ingressLinkID: String
        ) {
            device?.let { bluetoothDevice ->
                // Get current RSSI for this device and update if available
                val currentRSSI = connectionTracker.getBestRSSI(bluetoothDevice.address)
                if (currentRSSI != null) {
                    delegate?.onRSSIUpdated(bluetoothDevice.address, currentRSSI)
                }
            }

            if (peerID == myPeerID) return // Ignore messages from self

            delegate?.onPacketReceived(packet, peerID, device, ingressLinkID)
        }
        
        override fun onDeviceConnected(device: BluetoothDevice) {
            // Trigger limit enforcement immediately upon any new connection
            enforceStrictLimits()
            delegate?.onDeviceConnected(device)
        }

        override fun onDeviceDisconnected(device: BluetoothDevice, linkID: String?, peerID: String?) {
            packetBroadcaster.onLinkDisconnected(device.address, linkID)
            delegate?.onDeviceDisconnected(device, linkID, peerID)
        }

        override fun onGattClientWriteComplete(deviceAddress: String, linkID: String, status: Int) {
            packetBroadcaster.onGattClientWriteComplete(deviceAddress, linkID, status)
        }

        override fun onGattServerNotificationComplete(deviceAddress: String, linkID: String?, status: Int) {
            packetBroadcaster.onGattServerNotificationComplete(deviceAddress, linkID, status)
        }
        
        override fun onRSSIUpdated(deviceAddress: String, rssi: Int) {
            delegate?.onRSSIUpdated(deviceAddress, rssi)
        }
    }
    
    private val serverManager = BluetoothGattServerManager(
        context, connectionScope, connectionTracker, permissionManager, powerManager, componentDelegate, myPeerID
    )
    private val clientManager = BluetoothGattClientManager(
        context, connectionScope, connectionTracker, permissionManager, powerManager, componentDelegate
    )
    
    // Service state
    private var isActive = false
    
    // Delegate for callbacks
    var delegate: BluetoothConnectionManagerDelegate? = null

    /**
     * Dynamically-registered receiver that fires whenever the Bluetooth adapter is toggled
     * off/on by the user or system. Used to force a scan restart when BT comes back on,
     * because the BLE stack invalidates all scanner callbacks when the radio goes off.
     *
     * Null when not registered. Registration/unregistration happens synchronously from
     * start/stop, and onReceive only forwards a single call to
     * [onBluetoothAdapterStateChanged].
     */
    private var bluetoothStateReceiver: BluetoothStateReceiver? = null

    
    // Public property for address-peer mapping
    val addressPeerMap get() = connectionTracker.addressPeerMap

    fun observePeerIfCurrent(deviceAddress: String, linkID: String, peerID: String): Boolean =
        connectionTracker.observePeerIfCurrent(deviceAddress, linkID, peerID)

    fun getCurrentLinkID(deviceAddress: String): String? =
        connectionTracker.getCurrentLinkID(deviceAddress)

    private fun isBleTransportEnabled(): Boolean {
        return try {
            com.bitchat.android.ui.debug.DebugSettingsManager.getInstance().bleEnabled.value
        } catch (_: Exception) {
            try { com.bitchat.android.ui.debug.DebugPreferenceManager.getBleEnabled(true) } catch (_: Exception) { true }
        }
    }

    private fun isGattServerEnabled(): Boolean {
        return isBleTransportEnabled() &&
            (try { com.bitchat.android.ui.debug.DebugSettingsManager.getInstance().gattServerEnabled.value } catch (_: Exception) { true })
    }

    private fun isGattClientEnabled(): Boolean {
        return isBleTransportEnabled() &&
            (try { com.bitchat.android.ui.debug.DebugSettingsManager.getInstance().gattClientEnabled.value } catch (_: Exception) { true })
    }

    init {
        connectionScope.launch {
            var previousMode: PowerManager.PowerMode? = null
            powerManager.profile.collect { profile ->
                val modeChanged = previousMode != null && previousMode != profile.mode
                previousMode = profile.mode
                if (!isActive || !isBleTransportEnabled()) return@collect

                if (modeChanged && isGattServerEnabled()) {
                    serverManager.restartAdvertising()
                }
                clientManager.applyPowerProfile(profile)
            }
        }
        // Observe debug settings to enforce role state while active
        try {
            val dbg = com.bitchat.android.ui.debug.DebugSettingsManager.getInstance()
            // Master transport enable/disable
            connectionScope.launch {
                dbg.bleEnabled.collect { enabled ->
                    if (enabled) return@collect
                    if (isActive) {
                        disableTransport()
                    }
                }
            }
            // Role enable/disable
            connectionScope.launch {
                dbg.gattServerEnabled.collect { enabled ->
                    if (!isActive) return@collect
                    if (enabled && isBleTransportEnabled()) startServer() else stopServer()
                }
            }
            connectionScope.launch {
                dbg.gattClientEnabled.collect { enabled ->
                    if (!isActive) return@collect
                    if (enabled && isBleTransportEnabled()) startClient() else stopClient()
                }
            }
            
            // Centralized limit enforcement on any setting change
            connectionScope.launch {
                combine(
                    dbg.maxConnectionsOverall,
                    dbg.maxServerConnections,
                    dbg.maxClientConnections
                ) { _, _, _ -> 
                    // We don't need the values here, we just need to trigger enforcement
                    Unit 
                }.collect {
                    if (isActive) {
                        enforceStrictLimits()
                    }
                }
            }
        } catch (_: Exception) { }
    }
    
    /**
     * Centralized connection limit enforcement
     */
    private fun enforceStrictLimits() {
        if (!isActive) return
        
        try {
            val dbg = com.bitchat.android.ui.debug.DebugSettingsManager.getInstance()
            val maxOverall = dbg.maxConnectionsOverall.value
            val maxServer = dbg.maxServerConnections.value
            val maxClient = dbg.maxClientConnections.value
            
            // Get list of connections to evict to satisfy all constraints
            val toEvict = connectionTracker.getConnectionsToEvict(maxOverall, maxServer, maxClient)
            
            if (toEvict.isNotEmpty()) {
                Log.i(TAG, "Enforcing limits (max: $maxOverall, s: $maxServer, c: $maxClient) - evicting ${toEvict.size} connections")
                
                toEvict.forEach { conn ->
                    if (conn.isClient) {
                        try { conn.gatt?.disconnect() } catch (_: Exception) { }
                    } else {
                        serverManager.disconnectDevice(conn.device)
                    }
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "Error enforcing limits: ${e.message}")
        }
    }
    
    /**
     * Start all Bluetooth services with power optimization
     */
    fun startServices(): Boolean {
        if (!isBleTransportEnabled()) {
            Log.i(TAG, "BLE transport disabled by debug settings; not starting Bluetooth services")
            disableTransport()
            return false
        }
        
        if (!permissionManager.hasBluetoothPermissions()) {
            Log.e(TAG, "Missing Bluetooth permissions")
            return false
        }
        
        if (bluetoothAdapter?.isEnabled != true) {
            Log.e(TAG, "Bluetooth is not enabled")
            // Even though we can't start scanning right now, register the state receiver
            // so that when the user turns Bluetooth on, we automatically (re)start services
            // and force a scan. This is the core of the "detect BT on/off and force scan"
            // feature: a mesh service that was started while the radio was off will still
            // react to the radio coming back.
            registerBluetoothStateReceiver()
            return false
        }
        
        try {
            isActive = true

        // set the adapter's name to our 8-character peerID for iOS privacy, TODO: Make this configurable
        // try {
        //     if (bluetoothAdapter?.name != myPeerID) {
        //         bluetoothAdapter?.name = myPeerID
        //         Log.d(TAG, "Set Bluetooth adapter name to peerID: $myPeerID for iOS compatibility.")
        //     }
        // } catch (se: SecurityException) {
        //     Log.e(TAG, "Missing BLUETOOTH_CONNECT permission to set adapter name.", se)
        // }

            // Register the BT state receiver so a later off->on toggle forces a scan restart.
            // Registered here (rather than in init) so it only fires while the mesh is alive.
            registerBluetoothStateReceiver()

            // Start all component managers
            connectionScope.launch {
                // Start connection tracker first
                connectionTracker.start()
                
                // Start power manager
                powerManager.start()
                
                // Start server/client based on debug settings
                val startServer = isGattServerEnabled()
                val startClient = isGattClientEnabled()

                if (startServer) {
                    if (!serverManager.start()) {
                        Log.e(TAG, "Failed to start server manager")
                        this@BluetoothConnectionManager.isActive = false
                        return@launch
                    }
                } else {
                    Log.i(TAG, "GATT Server disabled by debug settings; not starting")
                }

                if (startClient) {
                    if (!clientManager.start()) {
                        Log.e(TAG, "Failed to start client manager")
                        this@BluetoothConnectionManager.isActive = false
                        return@launch
                    }
                } else {
                    Log.i(TAG, "GATT Client disabled by debug settings; not starting")
                }
                
                Log.i(TAG, "Bluetooth services started successfully")
            }
            
            return true
            
        } catch (e: Exception) {
            Log.e(TAG, "Failed to start Bluetooth services: ${e.message}")
            isActive = false
            return false
        }
    }

    /**
     * Register [BluetoothStateReceiver] to listen for BluetoothAdapter.ACTION_STATE_CHANGED.
     * Idempotent: calling it while a receiver is already registered is a no-op.
     *
     * On Android 13+ (API 33+) we must specify an export flag for dynamic receivers.
     * ACTION_STATE_CHANGED is a system/protected broadcast, so we use RECEIVER_NOT_EXPORTED.
     */
    private fun registerBluetoothStateReceiver() {
        if (bluetoothStateReceiver != null) {
            Log.d(TAG, "BluetoothStateReceiver already registered; skipping")
            return
        }
        val receiver = BluetoothStateReceiver { state ->
            onBluetoothAdapterStateChanged(state)
        }
        val filter = IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED)
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                ContextCompat.registerReceiver(
                    context,
                    receiver,
                    filter,
                    ContextCompat.RECEIVER_NOT_EXPORTED
                )
            } else {
                @Suppress("UnspecifiedRegisterReceiverFlag")
                context.registerReceiver(receiver, filter)
            }
            bluetoothStateReceiver = receiver
            Log.i(TAG, "BluetoothStateReceiver registered (will force-scan on BT ON)")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to register BluetoothStateReceiver: ${e.message}", e)
        }
    }

    /**
     * Unregister [BluetoothStateReceiver] if previously registered. Safe to call from any
     * state (including when never registered or already unregistered).
     */
    private fun unregisterBluetoothStateReceiver() {
        val receiver = bluetoothStateReceiver ?: return
        try {
            context.unregisterReceiver(receiver)
            Log.i(TAG, "BluetoothStateReceiver unregistered")
        } catch (e: Exception) {
            // Common when the receiver was already unregistered; not fatal.
            Log.w(TAG, "Failed to unregister BluetoothStateReceiver: ${e.message}")
        }
        bluetoothStateReceiver = null
    }

    /**
     * Public handler for Bluetooth adapter state changes.
     *
     * Called by [BluetoothStateReceiver] whenever the adapter transitions between OFF / ON
     * (and the intermediate TURNING_* states). The most important transition is OFF -> ON:
     * the BLE stack has just torn down all scanner callbacks and rebuilt the radio, so the
     * client manager's "isCurrentlyScanning" flag is stale and the scanner is no longer
     * running. We force a clean restart here so peer discovery resumes immediately instead
     * of waiting for (or never getting) a watchdog recovery.
     *
     * Also exposes [forceBluetoothScanRestart] publicly for callers (debug UI, tests) that
     * want to manually trigger the same recovery path.
     *
     * @param state the new [BluetoothAdapter] state (STATE_OFF, STATE_TURNING_OFF,
     * STATE_ON, STATE_TURNING_ON, ...)
     */
    fun onBluetoothAdapterStateChanged(state: Int) {
        when (state) {
            BluetoothAdapter.STATE_ON -> {
                Log.i(TAG, "Bluetooth adapter turned ON -> forcing BLE scan restart")
                if (isActive) {
                    // Re-arm advertising in case the server side was also wedged.
                    if (isGattServerEnabled()) {
                        try { serverManager.restartAdvertising() } catch (e: Exception) {
                            Log.w(TAG, "restartAdvertising failed: ${e.message}")
                        }
                    }
                    // The core fix: force the client scanner to restart now.
                    if (isGattClientEnabled()) {
                        try { clientManager.forceScanRestart() } catch (e: Exception) {
                            Log.w(TAG, "forceScanRestart failed: ${e.message}")
                        }
                    }
                } else {
                    // The mesh service was started while BT was off (startServices() returned
                    // false). Try again now that the adapter is on.
                    Log.i(TAG, "Mesh inactive but BT just came on -> attempting startServices()")
                    startServices()
                }
            }
            BluetoothAdapter.STATE_TURNING_OFF -> {
                Log.i(TAG, "Bluetooth adapter turning OFF -> scan will be stopped by system")
                // System stops the scan for us; our flags will be corrected on the next
                // STATE_ON by forceScanRestart(). Nothing to do here.
            }
            BluetoothAdapter.STATE_OFF -> {
                Log.i(TAG, "Bluetooth adapter is OFF")
            }
            BluetoothAdapter.STATE_TURNING_ON -> {
                Log.d(TAG, "Bluetooth adapter turning ON (waiting for STATE_ON to scan)")
            }
            else -> {
                Log.d(TAG, "Bluetooth adapter state: $state")
            }
        }
    }

    /**
     * Manually trigger a forced BLE scan restart. Exposed for debug UI / tests so the same
     * recovery path used on BT ON can be invoked without actually toggling the radio.
     */
    fun forceBluetoothScanRestart() {
        if (isActive && isGattClientEnabled()) {
            Log.i(TAG, "Manual forceBluetoothScanRestart requested")
            try { clientManager.forceScanRestart() } catch (e: Exception) {
                Log.w(TAG, "Manual forceScanRestart failed: ${e.message}")
            }
        } else {
            Log.w(TAG, "forceBluetoothScanRestart ignored: inactive or client disabled")
        }
    }


    /**
     * Disable BLE without cancelling this manager's coroutine scope, so it can be re-enabled.
     */
    fun disableTransport() {
        Log.i(TAG, "Disabling BLE transport")
        isActive = false
        unregisterBluetoothStateReceiver()
        connectionScope.launch {
            clientManager.stop()
            serverManager.stop()
            connectionTracker.stop()
        }
    }
    
    /**
     * Stop all Bluetooth services with proper cleanup
     */
    fun stopServices() {
        isActive = false
        unregisterBluetoothStateReceiver()

        connectionScope.launch {
            // Stop component managers
            clientManager.stop()
            serverManager.stop()
            
            // Stop connection tracker
            connectionTracker.stop()
            
            // Cancel the coroutine scope
            connectionScope.cancel()
            
            Log.i(TAG, "All Bluetooth services stopped")
        }
    }

    /**
     * Indicates whether this instance can be safely reused for a future start.
     * Returns false if its coroutine scope has been cancelled.
     */
    fun isReusable(): Boolean {
        return connectionScope.isActive
    }
    
    /**
     * Broadcast packet to connected devices with connection limit enforcement
     * Automatically fragments large packets to fit within BLE MTU limits
     */
    fun broadcastPacket(routed: RoutedPacket): Boolean {
        if (!isActive || !isBleTransportEnabled()) return false

        return packetBroadcaster.broadcastPacket(
            routed,
            serverManager.getGattServer(),
            serverManager.getCharacteristic()
        )
    }

    suspend fun broadcastControlPacketAndAwaitAcceptance(routed: RoutedPacket): Boolean {
        if (!isActive || !isBleTransportEnabled()) return false

        return packetBroadcaster.broadcastControlPacketAndAwaitAcceptance(
            routed,
            serverManager.getGattServer(),
            serverManager.getCharacteristic()
        )
    }

    fun sendToPeer(peerID: String, routed: RoutedPacket): Boolean {
        if (!isActive || !isBleTransportEnabled()) return false
        return packetBroadcaster.sendToPeer(
            peerID,
            routed,
            serverManager.getGattServer(),
            serverManager.getCharacteristic()
        )
    }

    fun cancelTransfer(transferId: String): Boolean {
        return packetBroadcaster.cancelTransfer(transferId)
    }

    /**
     * Send a packet directly to a specific peer, without broadcasting to others.
     */
    fun sendPacketToPeer(peerID: String, packet: BitchatPacket): Boolean {
        if (!isActive || !isBleTransportEnabled()) return false
        return packetBroadcaster.sendPacketToPeer(
            RoutedPacket(packet),
            peerID,
            serverManager.getGattServer(),
            serverManager.getCharacteristic()
        )
    }

    fun sendPacketToLink(deviceAddress: String, linkID: String, packet: BitchatPacket): Boolean {
        if (!isActive || !isBleTransportEnabled()) return false
        return packetBroadcaster.sendPacketToLink(
            RoutedPacket(packet),
            deviceAddress,
            linkID,
            serverManager.getGattServer(),
            serverManager.getCharacteristic()
        )
    }
    

    // Expose role controls for debug UI
    fun startServer() {
        if (!isActive || !isBleTransportEnabled()) return
        connectionScope.launch { if (isGattServerEnabled()) serverManager.start() }
    }
    fun stopServer() { connectionScope.launch { serverManager.stop() } }
    fun startClient() {
        if (!isActive || !isBleTransportEnabled()) return
        connectionScope.launch { if (isGattClientEnabled()) clientManager.start() }
    }
    fun stopClient() { connectionScope.launch { clientManager.stop() } }

    // Inject nickname resolver for broadcaster logs
    fun setNicknameResolver(resolver: (String) -> String?) { packetBroadcaster.setNicknameResolver(resolver) }

    // Debug snapshots for connected devices
    fun getConnectedDeviceEntries(): List<Triple<String, Boolean, Int?>> {
        return try {
            connectionTracker.getConnectedDevices().values.map { dc ->
                val rssi = if (dc.rssi != Int.MIN_VALUE) dc.rssi else null
                Triple(dc.device.address, dc.isClient, rssi)
            }
        } catch (e: Exception) {
            emptyList()
        }
    }

    // Expose local adapter address for debug UI
    fun getLocalAdapterAddress(): String? = try { bluetoothAdapter?.address } catch (e: Exception) { null }

    fun isClientConnection(address: String): Boolean? {
        return try { connectionTracker.getConnectedDevices()[address]?.isClient } catch (e: Exception) { null }
    }

    /**
     * Public: connect/disconnect helpers for debug UI
     */
    fun connectToAddress(address: String): Boolean {
        if (!isActive || !isBleTransportEnabled()) return false
        return clientManager.connectToAddress(address)
    }
    fun disconnectAddress(address: String) { connectionTracker.disconnectDevice(address) }


    // Optionally disconnect all connections (server and client)
    fun disconnectAll() {
        connectionScope.launch {
            // Stop and restart to force disconnects
            clientManager.stop()
            serverManager.stop()
            delay(200)
            if (isActive && isBleTransportEnabled()) {
                // Restart managers if service is active
                if (isGattServerEnabled()) serverManager.start()
                if (isGattClientEnabled()) clientManager.start()
            }
        }
    }


    /**
     * Get connected device count
     */
    fun getConnectedDeviceCount(): Int = connectionTracker.getConnectedDeviceCount()
    
    /**
     * Get debug information including power management
     */
    fun getDebugInfo(): String {
        return buildString {
            appendLine("=== Bluetooth Connection Manager ===")
            appendLine("Bluetooth MAC Address: ${bluetoothAdapter?.address}")
            appendLine("Active: $isActive")
            appendLine("Bluetooth Enabled: ${bluetoothAdapter?.isEnabled}")
            appendLine("Has Permissions: ${permissionManager.hasBluetoothPermissions()}")
            appendLine("GATT Server Active: ${serverManager.getGattServer() != null}")
            appendLine()
            appendLine(powerManager.getPowerInfo())
            appendLine()
            appendLine(connectionTracker.getDebugInfo())
        }
    }
    
}

/**
 * Delegate interface for Bluetooth connection manager callbacks
 */
interface BluetoothConnectionManagerDelegate {
    fun onPacketReceived(
        packet: BitchatPacket,
        peerID: String,
        device: BluetoothDevice?,
        ingressLinkID: String
    )
    fun onDeviceConnected(device: BluetoothDevice)
    fun onDeviceDisconnected(device: BluetoothDevice, linkID: String?, peerID: String?)
    fun onRSSIUpdated(deviceAddress: String, rssi: Int)
    fun onGattClientWriteComplete(deviceAddress: String, linkID: String, status: Int) = Unit
    fun onGattServerNotificationComplete(deviceAddress: String, linkID: String?, status: Int) = Unit
}
