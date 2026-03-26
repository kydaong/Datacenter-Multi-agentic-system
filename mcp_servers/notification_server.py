"""
Notification Server
Send alerts and notifications via multiple channels
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Optional
from datetime import datetime
from enum import Enum
import os
from dotenv import load_dotenv

load_dotenv()


class NotificationChannel(Enum):
    """Notification channels"""
    EMAIL = "email"
    SMS = "sms"
    SLACK = "slack"
    TEAMS = "teams"


class NotificationPriority(Enum):
    """Notification priority levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class NotificationServer:
    """
    MCP Server for sending notifications
    Supports email, SMS, Slack, Teams
    """
    
    def __init__(self):
        """Initialize notification server"""
        
        # Email configuration
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', 587))
        self.smtp_user = os.getenv('SMTP_USER', '')
        self.smtp_password = os.getenv('SMTP_PASSWORD', '')
        self.from_email = os.getenv('FROM_EMAIL', 'noreply@chilleroptim.com')
        
        # Notification recipients (by priority)
        self.recipients = {
            NotificationPriority.CRITICAL: os.getenv('CRITICAL_RECIPIENTS', '').split(','),
            NotificationPriority.HIGH: os.getenv('HIGH_RECIPIENTS', '').split(','),
            NotificationPriority.MEDIUM: os.getenv('MEDIUM_RECIPIENTS', '').split(','),
            NotificationPriority.LOW: os.getenv('LOW_RECIPIENTS', '').split(',')
        }
        
        print(" Notification Server initialized")
    
    def send_notification(
        self,
        subject: str,
        message: str,
        priority: NotificationPriority = NotificationPriority.MEDIUM,
        channels: List[NotificationChannel] = None,
        recipients: Optional[List[str]] = None
    ) -> Dict:
        """
        Send notification across specified channels
        
        Args:
            subject: Notification subject
            message: Notification message
            priority: Priority level
            channels: Channels to use (defaults to EMAIL)
            recipients: Override recipients
        
        Returns:
            Send results
        """
        
        channels = channels or [NotificationChannel.EMAIL]
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'subject': subject,
            'priority': priority.value,
            'channels': {}
        }
        
        # Send via each channel
        for channel in channels:
            if channel == NotificationChannel.EMAIL:
                result = self._send_email(subject, message, priority, recipients)
                results['channels']['email'] = result
            
            elif channel == NotificationChannel.SMS:
                result = self._send_sms(message, priority, recipients)
                results['channels']['sms'] = result
            
            elif channel == NotificationChannel.SLACK:
                result = self._send_slack(subject, message, priority)
                results['channels']['slack'] = result
            
            elif channel == NotificationChannel.TEAMS:
                result = self._send_teams(subject, message, priority)
                results['channels']['teams'] = result
        
        return results
    
    def send_alarm_notification(
        self,
        equipment_id: str,
        alarm_code: str,
        alarm_description: str,
        severity: str,
        triggered_value: float,
        threshold_value: float
    ) -> Dict:
        """
        Send equipment alarm notification
        
        Args:
            equipment_id: Equipment identifier
            alarm_code: Alarm code
            alarm_description: Description
            severity: Severity (WARNING, CRITICAL)
            triggered_value: Actual value
            threshold_value: Threshold
        
        Returns:
            Send result
        """
        
        subject = f"[{severity}] {equipment_id} - {alarm_code}"
        
        message = f"""
Equipment Alarm Triggered

Equipment: {equipment_id}
Alarm Code: {alarm_code}
Description: {alarm_description}
Severity: {severity}

Triggered Value: {triggered_value}
Threshold: {threshold_value}

Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Please investigate immediately.
        """
        
        priority = NotificationPriority.CRITICAL if severity == 'CRITICAL' else NotificationPriority.HIGH
        
        return self.send_notification(
            subject=subject,
            message=message,
            priority=priority,
            channels=[NotificationChannel.EMAIL, NotificationChannel.SLACK]
        )
    
    def send_decision_approval_request(
        self,
        decision_id: str,
        proposed_by: str,
        action_type: str,
        description: str,
        predicted_savings: Dict
    ) -> Dict:
        """
        Send decision approval request to operations team
        
        Args:
            decision_id: Decision ID
            proposed_by: Agent that proposed
            action_type: Action type
            description: Action description
            predicted_savings: Predicted benefits
        
        Returns:
            Send result
        """
        
        subject = f"Agent Decision Approval Required - {action_type}"
        
        message = f"""
Agent Decision Requiring Approval

Decision ID: {decision_id}
Proposed By: {proposed_by}
Action Type: {action_type}

Description:
{description}

Predicted Benefits:
- Energy Savings: {predicted_savings.get('energy_kw', 'N/A')} kW
- Cost Savings: SGD {predicted_savings.get('cost_sgd', 'N/A')}
- PUE Improvement: {predicted_savings.get('pue_improvement', 'N/A')}

Please review and approve via the dashboard.

Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        
        return self.send_notification(
            subject=subject,
            message=message,
            priority=NotificationPriority.HIGH,
            channels=[NotificationChannel.EMAIL]
        )
    
    def send_performance_report(
        self,
        report_type: str,
        metrics: Dict
    ) -> Dict:
        """
        Send performance report
        
        Args:
            report_type: Report type (daily, weekly, monthly)
            metrics: Performance metrics
        
        Returns:
            Send result
        """
        
        subject = f"Chiller Plant {report_type.title()} Performance Report"
        
        message = f"""
{report_type.title()} Performance Report

Period: {metrics.get('period', 'N/A')}

Key Metrics:
- Average PUE: {metrics.get('avg_pue', 'N/A')}
- Total Energy Consumption: {metrics.get('total_energy_kwh', 'N/A')} kWh
- Total Cost: SGD {metrics.get('total_cost_sgd', 'N/A')}
- Average Plant Efficiency: {metrics.get('avg_plant_efficiency', 'N/A')} kW/ton
- Uptime: {metrics.get('uptime_percent', 'N/A')}%

Agent Activity:
- Decisions Proposed: {metrics.get('decisions_proposed', 'N/A')}
- Decisions Executed: {metrics.get('decisions_executed', 'N/A')}
- Total Savings: SGD {metrics.get('total_savings_sgd', 'N/A')}

For detailed analysis, see the dashboard.

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        
        return self.send_notification(
            subject=subject,
            message=message,
            priority=NotificationPriority.MEDIUM,
            channels=[NotificationChannel.EMAIL]
        )
    
    def _send_email(
        self,
        subject: str,
        message: str,
        priority: NotificationPriority,
        recipients: Optional[List[str]] = None
    ) -> Dict:
        """
        Send email notification
        """
        
        # Get recipients
        to_addresses = recipients or self.recipients.get(priority, [])
        to_addresses = [addr.strip() for addr in to_addresses if addr.strip()]
        
        if not to_addresses:
            return {
                'success': False,
                'error': 'No recipients configured'
            }
        
        # For development, just log (don't send real email)
        if not self.smtp_user or not self.smtp_password or 'dummy' in self.smtp_password.lower():
            print(f"[EMAIL] To: {', '.join(to_addresses)}")
            print(f"[EMAIL] Subject: {subject}")
            print(f"[EMAIL] Message: {message[:100]}...")
            
            return {
                'success': True,
                'recipients': to_addresses,
                'note': 'Email simulation (SMTP not configured)'
            }
        
        # Send real email
        try:
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = ', '.join(to_addresses)
            msg['Subject'] = subject
            
            msg.attach(MIMEText(message, 'plain'))
            
            server = smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=10)
            server.starttls()
            server.login(self.smtp_user, self.smtp_password)
            server.send_message(msg)
            server.quit()
            
            return {
                'success': True,
                'recipients': to_addresses
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def _send_sms(
        self,
        message: str,
        priority: NotificationPriority,
        recipients: Optional[List[str]] = None
    ) -> Dict:
        """
        Send SMS notification (Twilio/SNS integration)
        """
        
        # In production, integrate with Twilio or AWS SNS
        print(f"[SMS] {message[:50]}...")
        
        return {
            'success': True,
            'note': 'SMS simulation (Twilio not configured)'
        }
    
    def _send_slack(
        self,
        subject: str,
        message: str,
        priority: NotificationPriority
    ) -> Dict:
        """
        Send Slack notification
        """
        
        # In production, integrate with Slack API
        print(f"[SLACK] {subject}")
        
        return {
            'success': True,
            'note': 'Slack simulation (Webhook not configured)'
        }
    
    def _send_teams(
        self,
        subject: str,
        message: str,
        priority: NotificationPriority
    ) -> Dict:
        """
        Send Microsoft Teams notification
        """
        
        # In production, integrate with Teams webhook
        print(f"[TEAMS] {subject}")
        
        return {
            'success': True,
            'note': 'Teams simulation (Webhook not configured)'
        }


# Example usage
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING NOTIFICATION SERVER")
    print("="*70)
    
    # Initialize server
    notif = NotificationServer()
    
    # Test 1: Send alarm
    print("\n[TEST 1] Send Alarm Notification:")
    result = notif.send_alarm_notification(
        equipment_id='Chiller-1',
        alarm_code='ALM-CH-001',
        alarm_description='High discharge pressure',
        severity='CRITICAL',
        triggered_value=13.5,
        threshold_value=12.5
    )
    print(f"  Success: {result['channels']['email']['success']}")
    
    # Test 2: Send approval request
    print("\n[TEST 2] Send Approval Request:")
    result = notif.send_decision_approval_request(
        decision_id='DEC-001',
        proposed_by='Chiller Optimization Agent',
        action_type='CHILLER_STAGING',
        description='Stage Chiller-3 online to improve part-load efficiency',
        predicted_savings={
            'energy_kw': 50,
            'cost_sgd': 120,
            'pue_improvement': 0.02
        }
    )
    print(f"  Success: {result['channels']['email']['success']}")
    
    # Test 3: Send performance report
    print("\n[TEST 3] Send Performance Report:")
    result = notif.send_performance_report(
        report_type='daily',
        metrics={
            'period': '2025-01-15',
            'avg_pue': 1.22,
            'total_energy_kwh': 24500,
            'total_cost_sgd': 4900,
            'avg_plant_efficiency': 0.54,
            'uptime_percent': 99.8,
            'decisions_proposed': 12,
            'decisions_executed': 8,
            'total_savings_sgd': 450
        }
    )
    print(f"  Success: {result['channels']['email']['success']}")
    
    print("\n Notification Server tests complete!")