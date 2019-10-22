package fredboat.config.property

class RatelimitConfig(
        val balancingBlock: String = "",
        val excludedIps: List<String> = emptyList(),
        val strategy: String = "RotateOnBan"
)