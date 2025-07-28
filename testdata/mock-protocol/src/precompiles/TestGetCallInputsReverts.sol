//contract TriggeringTx {
//    constructor() payable {
//        TARGET.writeStorage(1);
//
//        (bool success, ) = address(this).call(
//            abi.encodeWithSelector(this.reverts.selector)
//        );
//        require(success);
//
//        TARGET.writeStorage(1);
//    }
//
//    function reverts() external pure {
//        revert("revert");
//    }
//}
//