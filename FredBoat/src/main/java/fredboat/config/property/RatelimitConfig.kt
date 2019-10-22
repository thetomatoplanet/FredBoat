package fredboat.config.property

class RatelimitConfig(
        var balancingBlock: String = "",
        var excludedIps: List<String> = emptyList(),
        var strategy: String = "RotateOnBan"
)