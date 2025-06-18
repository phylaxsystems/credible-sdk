import {Script} from "forge-std/Script.sol";
import {console} from "forge-std/console.sol";

contract Deploy is Script {
    function run(address owner) public {
        vm.broadcast();
        address a = address(new Ownable(owner));
        console.log("Deployed Ownable contract at address: ", a);
    }
}

contract Ownable {
    address public owner;

    constructor(address _owner) {
        owner = _owner;
    }
}
